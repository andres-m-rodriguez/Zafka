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

pub const RequestBody = union(enum) {
    none: void,
    describe_topic_partitions: DescribeTopicPartitionsRequestBody,
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
