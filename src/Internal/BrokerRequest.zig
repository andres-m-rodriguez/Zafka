const std = @import("std");



pub const RequestHeader = struct {
    api_key: u16,
    api_version: u16,
    correlation_id: i32,
    client_id: ?[]const u8,
};

pub const DescribeTopicPartitionsRequestBody = struct {
    topic_names: []const []const u8,
};

pub const FetchTopicRequest = struct {
    topic_id: [16]u8,
    partition_indexes: []const i32,
};

pub const FetchRequestBody = struct {
    topics: []const FetchTopicRequest,
};

pub const ProducePartitionRequest = struct {
    partition_index: i32,
};

pub const ProduceTopicRequest = struct {
    name: []const u8,
    partitions: []const ProducePartitionRequest,
};

pub const ProduceRequestBody = struct {
    topics: []const ProduceTopicRequest,
};

pub const RequestBody = union(enum) {
    none: void,
    describe_topic_partitions: DescribeTopicPartitionsRequestBody,
    fetch: FetchRequestBody,
    produce: ProduceRequestBody,
};

pub const BrokerRequest = struct {
    message_size: i32,
    headers: RequestHeader,
    body: RequestBody,
};

const ParsingState = struct {
    state: State = .Parsing_api_key,
    // For tracking array parsing progress
    topics_remaining: usize = 0,
    current_topic_index: usize = 0,

    const State = enum {
        // Header states
        Parsing_api_key,
        Parsing_api_version,
        Parsing_correlation_id,
        Parsing_client_id,
        Parsing_header_tag_buffer,
        // Body states for DescribeTopicPartitions (api_key=75)
        Parsing_topics_array_length,
        Parsing_topic_name_length,
        Parsing_topic_name,
        Parsing_topic_tag_buffer,
        // Body states for Fetch (api_key=1)
        Parsing_fetch_body,
        // Body states for Produce (api_key=0)
        Parsing_produce_body,
        // Terminal state
        Done,
    };

    pub fn next(self: *ParsingState, api_key: u16) void {
        switch (self.state) {
            .Parsing_api_key => self.state = .Parsing_api_version,
            .Parsing_api_version => self.state = .Parsing_correlation_id,
            .Parsing_correlation_id => self.state = .Parsing_client_id,
            .Parsing_client_id => self.state = .Parsing_header_tag_buffer,
            .Parsing_header_tag_buffer => {
                // Transition to body parsing based on api_key
                if (api_key == 75) {
                    self.state = .Parsing_topics_array_length;
                } else if (api_key == 1) {
                    self.state = .Parsing_fetch_body;
                } else if (api_key == 0) {
                    self.state = .Parsing_produce_body;
                } else {
                    self.state = .Done;
                }
            },
            .Parsing_topics_array_length => self.state = .Parsing_topic_name_length,
            .Parsing_topic_name_length => self.state = .Parsing_topic_name,
            .Parsing_topic_name => self.state = .Parsing_topic_tag_buffer,
            .Parsing_topic_tag_buffer => {
                self.current_topic_index += 1;
                if (self.current_topic_index < self.topics_remaining) {
                    self.state = .Parsing_topic_name_length;
                } else {
                    self.state = .Done;
                }
            },
            .Parsing_fetch_body => self.state = .Done,
            .Parsing_produce_body => self.state = .Done,
            .Done => {},
        }
    }
};

pub const Error = std.Io.Reader.Error || error{RequestIncomplete};

pub fn parseRequest(allocator: std.mem.Allocator, reader: *std.Io.Reader) !BrokerRequest {
    var parsing_state = ParsingState{};
    std.debug.print("Parsing request", .{});

    const message_size_bytes = try reader.take(4);
    const message_size = std.mem.readInt(i32, message_size_bytes[0..4], .big);
    var broker_request = BrokerRequest{
        .message_size = message_size,
        .headers = RequestHeader{
            .api_key = 0,
            .api_version = 0,
            .correlation_id = 0,
            .client_id = null,
        },
        .body = .{ .none = {} },
    };

    // Body parsing state
    var topic_names: ?[][]u8 = null;
    var current_topic_name_len: usize = 0;
    var fetch_topics: ?[]FetchTopicRequest = null;
    var produce_topics: ?[]ProduceTopicRequest = null;

    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);
    var consumed: usize = 0;

    while (true) {
        const data = reader.peekGreedy(1) catch |err| {
            if (buffer.items.len < message_size) {
                return Error.RequestIncomplete;
            }
            return err;
        };
        reader.toss(data.len); // Consume it
        try buffer.appendSlice(allocator, data);

        while (true) {
            switch (parsing_state.state) {
                // === HEADER PARSING ===
                .Parsing_api_key => {
                    const parsing_results = parseApiKey(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const bytes = parsing_results.complete.value;
                        const ptr: *const [2]u8 = @ptrCast(bytes.ptr);
                        broker_request.headers.api_key = std.mem.readInt(u16, ptr, .big);
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },
                .Parsing_api_version => {
                    const parsing_results = parseApiVersion(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const bytes = parsing_results.complete.value;
                        const ptr: *const [2]u8 = @ptrCast(bytes.ptr);
                        broker_request.headers.api_version = std.mem.readInt(u16, ptr, .big);
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },
                .Parsing_correlation_id => {
                    const parsing_results = parseCorrelationId(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const bytes = parsing_results.complete.value;
                        const ptr: *const [4]u8 = @ptrCast(bytes.ptr);
                        broker_request.headers.correlation_id = std.mem.readInt(i32, ptr, .big);
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },
                .Parsing_client_id => {
                    const parsing_results = parseClientId(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        // We don't need to store client_id for now, just skip it
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },
                .Parsing_header_tag_buffer => {
                    const parsing_results = parseTagBuffer(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },

                // === BODY PARSING (DescribeTopicPartitions) ===
                .Parsing_topics_array_length => {
                    const parsing_results = parseCompactArrayLength(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const len_byte = parsing_results.complete.value[0];
                        parsing_state.topics_remaining = len_byte -| 1; // compact: actual = byte - 1

                        if (parsing_state.topics_remaining > 0) {
                            topic_names = try allocator.alloc([]u8, parsing_state.topics_remaining);
                            parsing_state.next(broker_request.headers.api_key);
                        } else {
                            // No topics, we're done
                            parsing_state.state = .Done;
                        }
                    } else break;
                },
                .Parsing_topic_name_length => {
                    const parsing_results = parseCompactStringLength(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const len_byte = parsing_results.complete.value[0];
                        current_topic_name_len = len_byte -| 1; // compact: actual = byte - 1
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },
                .Parsing_topic_name => {
                    const parsing_results = parseBytes(buffer.items[consumed..], current_topic_name_len);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const name = try allocator.alloc(u8, current_topic_name_len);
                        @memcpy(name, parsing_results.complete.value);
                        topic_names.?[parsing_state.current_topic_index] = name;
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },
                .Parsing_topic_tag_buffer => {
                    const parsing_results = parseTagBuffer(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        parsing_state.next(broker_request.headers.api_key);
                    } else break;
                },

                // === BODY PARSING (Fetch) ===
                .Parsing_fetch_body => {
                    // Parse Fetch v16 request body
                    // Skip: max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) + session_id(4) + session_epoch(4) = 21 bytes
                    const remaining = buffer.items[consumed..];
                    if (remaining.len < 22) break; // 21 + at least 1 for topics array length

                    var pos: usize = 21; // Skip the fixed fields

                    // Parse topics array (COMPACT_ARRAY)
                    const topics_len_byte = remaining[pos];
                    pos += 1;
                    const topics_count: usize = if (topics_len_byte > 0) topics_len_byte - 1 else 0;

                    if (topics_count > 0) {
                        fetch_topics = try allocator.alloc(FetchTopicRequest, topics_count);

                        for (0..topics_count) |topic_idx| {
                            // topic_id: UUID (16 bytes)
                            if (pos + 16 > remaining.len) break;
                            var topic_id: [16]u8 = undefined;
                            @memcpy(&topic_id, remaining[pos .. pos + 16]);
                            pos += 16;

                            // partitions: COMPACT_ARRAY
                            if (pos >= remaining.len) break;
                            const partitions_len_byte = remaining[pos];
                            pos += 1;
                            const partitions_count: usize = if (partitions_len_byte > 0) partitions_len_byte - 1 else 0;

                            var partition_indexes = try allocator.alloc(i32, partitions_count);

                            for (0..partitions_count) |part_idx| {
                                // partition: INT32
                                if (pos + 4 > remaining.len) break;
                                partition_indexes[part_idx] = std.mem.readInt(i32, remaining[pos..][0..4], .big);
                                pos += 4;

                                // Skip: current_leader_epoch(4) + fetch_offset(8) + last_fetched_epoch(4) + log_start_offset(8) + partition_max_bytes(4) = 28 bytes
                                pos += 28;

                                // Skip partition TAG_BUFFER
                                if (pos < remaining.len) pos += 1;
                            }

                            // Skip topic TAG_BUFFER
                            if (pos < remaining.len) pos += 1;

                            fetch_topics.?[topic_idx] = FetchTopicRequest{
                                .topic_id = topic_id,
                                .partition_indexes = partition_indexes,
                            };
                        }
                    } else {
                        fetch_topics = try allocator.alloc(FetchTopicRequest, 0);
                    }

                    consumed += pos;
                    parsing_state.next(broker_request.headers.api_key);
                },

                // === BODY PARSING (Produce) ===
                .Parsing_produce_body => {
                    // Parse Produce v11 request body
                    // Skip: transactional_id (COMPACT_NULLABLE_STRING) + acks (INT16) + timeout_ms (INT32)
                    const remaining = buffer.items[consumed..];
                    if (remaining.len < 1) break;

                    var pos: usize = 0;

                    // transactional_id (COMPACT_NULLABLE_STRING): 0 means null
                    const trans_id_len = remaining[pos];
                    pos += 1;
                    if (trans_id_len > 1) {
                        pos += trans_id_len - 1;
                    }

                    // acks (INT16)
                    if (pos + 2 > remaining.len) break;
                    pos += 2;

                    // timeout_ms (INT32)
                    if (pos + 4 > remaining.len) break;
                    pos += 4;

                    // topics array (COMPACT_ARRAY)
                    if (pos >= remaining.len) break;
                    const topics_len_byte = remaining[pos];
                    pos += 1;
                    const topics_count: usize = if (topics_len_byte > 0) topics_len_byte - 1 else 0;

                    if (topics_count > 0) {
                        produce_topics = try allocator.alloc(ProduceTopicRequest, topics_count);

                        for (0..topics_count) |topic_idx| {
                            // topic name (COMPACT_STRING)
                            if (pos >= remaining.len) break;
                            const name_len_byte = remaining[pos];
                            pos += 1;
                            const name_len: usize = if (name_len_byte > 0) name_len_byte - 1 else 0;

                            if (pos + name_len > remaining.len) break;
                            const name = try allocator.alloc(u8, name_len);
                            @memcpy(name, remaining[pos .. pos + name_len]);
                            pos += name_len;

                            // partitions array (COMPACT_ARRAY)
                            if (pos >= remaining.len) break;
                            const partitions_len_byte = remaining[pos];
                            pos += 1;
                            const partitions_count: usize = if (partitions_len_byte > 0) partitions_len_byte - 1 else 0;

                            var partitions = try allocator.alloc(ProducePartitionRequest, partitions_count);

                            for (0..partitions_count) |part_idx| {
                                // partition index (INT32)
                                if (pos + 4 > remaining.len) break;
                                const partition_index = std.mem.readInt(i32, remaining[pos..][0..4], .big);
                                pos += 4;

                                // records (COMPACT_BYTES) - skip
                                if (pos >= remaining.len) break;
                                // Read unsigned varint for records length
                                var records_len: u64 = 0;
                                var shift: u6 = 0;
                                while (pos < remaining.len) {
                                    const b = remaining[pos];
                                    pos += 1;
                                    records_len |= @as(u64, b & 0x7F) << shift;
                                    if ((b & 0x80) == 0) break;
                                    shift += 7;
                                }
                                if (records_len > 0) {
                                    pos += @intCast(records_len - 1); // -1 because COMPACT encoding
                                }

                                // Skip partition TAG_BUFFER
                                if (pos < remaining.len) pos += 1;

                                partitions[part_idx] = ProducePartitionRequest{
                                    .partition_index = partition_index,
                                };
                            }

                            // Skip topic TAG_BUFFER
                            if (pos < remaining.len) pos += 1;

                            produce_topics.?[topic_idx] = ProduceTopicRequest{
                                .name = name,
                                .partitions = partitions,
                            };
                        }
                    } else {
                        produce_topics = try allocator.alloc(ProduceTopicRequest, 0);
                    }

                    consumed += pos;
                    parsing_state.next(broker_request.headers.api_key);
                },

                .Done => break,
            }
        }

        if (parsing_state.state == .Done) {
            break;
        }
    }

    // Set the body if we parsed topic names
    if (topic_names) |names| {
        broker_request.body = .{
            .describe_topic_partitions = .{
                .topic_names = names,
            },
        };
    }

    // Set the body if we parsed fetch topics
    if (fetch_topics) |topics| {
        broker_request.body = .{
            .fetch = .{
                .topics = topics,
            },
        };
    }

    // Set the body if we parsed produce topics
    if (produce_topics) |topics| {
        broker_request.body = .{
            .produce = .{
                .topics = topics,
            },
        };
    }

    return broker_request;
}

const ParseResult = union(enum) {
    complete: struct {
        value: []const u8,
        bytes_consumed: usize,
    },
    incomplete: struct {
        bytes_read: usize,
    },

    pub fn Complete(value: []const u8, bytes_consumed: usize) ParseResult {
        return ParseResult{
            .complete = .{
                .value = value,
                .bytes_consumed = bytes_consumed,
            },
        };
    }
    pub fn Incomplete(bytes_consumed: usize) ParseResult {
        return ParseResult{
            .incomplete = .{
                .bytes_read = bytes_consumed,
            },
        };
    }
};
fn parseApiKey(buffer: []const u8) ParseResult {
    if (buffer.len < 2)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..2], 2);
}

fn parseApiVersion(buffer: []const u8) ParseResult {
    if (buffer.len < 2)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..2], 2);
}
fn parseCorrelationId(buffer: []const u8) ParseResult {
    if (buffer.len < 4)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..4], 4);
}

fn parseClientId(buffer: []const u8) ParseResult {
    // NULLABLE_STRING: INT16 length prefix, then string bytes
    // If length is -1 (0xFFFF), it's null
    if (buffer.len < 2)
        return ParseResult.Incomplete(buffer.len);

    const len_ptr: *const [2]u8 = @ptrCast(buffer[0..2]);
    const len = std.mem.readInt(i16, len_ptr, .big);

    if (len < 0) {
        // Null string, just consume the length bytes
        return ParseResult.Complete(buffer[0..2], 2);
    }

    const total_len: usize = 2 + @as(usize, @intCast(len));
    if (buffer.len < total_len)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..total_len], total_len);
}

fn parseTagBuffer(buffer: []const u8) ParseResult {
    // TAG_BUFFER: For now, we assume it's always empty (single 0x00 byte)
    // In a full implementation, this would be a varint followed by tagged fields
    if (buffer.len < 1)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..1], 1);
}

fn parseCompactArrayLength(buffer: []const u8) ParseResult {
    // COMPACT_ARRAY length: single byte where actual_length = byte - 1
    if (buffer.len < 1)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..1], 1);
}

fn parseCompactStringLength(buffer: []const u8) ParseResult {
    // COMPACT_STRING length: single byte where actual_length = byte - 1
    if (buffer.len < 1)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..1], 1);
}

fn parseBytes(buffer: []const u8, len: usize) ParseResult {
    if (buffer.len < len)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..len], len);
}
