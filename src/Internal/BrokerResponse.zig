const std = @import("std");
const brokerRequest = @import("BrokerRequest.zig");
const clusterMetadata = @import("ClusterMetadata.zig");

pub const ResponseHeader = struct {
    correlation_id: i32,
};

pub const ApiKeyEntry = struct {
    api_key: i16,
    min_version: i16,
    max_version: i16,
};

pub const ApiVersionsResponse = struct {
    error_code: i16,
    api_keys: []const ApiKeyEntry,
    throttle_time_ms: i32,
};

pub const PartitionResponse = struct {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: []const i32,
    isr_nodes: []const i32,
};

pub const TopicResponse = struct {
    error_code: i16,
    name: []const u8,
    topic_id: [16]u8, // UUID as 16 bytes
    is_internal: bool,
    partitions: []const PartitionResponse,
    topic_authorized_operations: i32,
};

pub const DescribeTopicPartitionsResponse = struct {
    throttle_time_ms: i32,
    topics: []const TopicResponse,
    // next_cursor: nullable, use -1 for null
};

pub const FetchPartitionResponse = struct {
    partition_index: i32,
    error_code: i16,
    records: ?[]const u8, // Raw record batch data from log file
};

pub const FetchTopicResponse = struct {
    topic_id: [16]u8,
    partitions: []const FetchPartitionResponse,
};

pub const FetchResponse = struct {
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i32,
    responses: []const FetchTopicResponse,
};

pub const BrokerResponse = struct {
    header: ResponseHeader,
    body: union(enum) {
        api_versions: ApiVersionsResponse,
        describe_topic_partitions: DescribeTopicPartitionsResponse,
        fetch: FetchResponse,
    },
};


const supported_api_keys = [_]ApiKeyEntry{
    ApiKeyEntry{
        .api_key = 1, // Fetch
        .min_version = 0,
        .max_version = 16,
    },
    ApiKeyEntry{
        .api_key = 18, // ApiVersions
        .min_version = 0,
        .max_version = 4,
    },
    ApiKeyEntry{
        .api_key = 75, // DescribeTopicPartitions
        .min_version = 0,
        .max_version = 0,
    },
};

fn readLogFile(allocator: std.mem.Allocator, path: []const u8) ?[]const u8 {
    const file = std.fs.openFileAbsolute(path, .{}) catch {
        return null;
    };
    defer file.close();

    const data = file.readToEndAlloc(allocator, 1024 * 1024) catch {
        return null;
    };

    if (data.len == 0) {
        return null;
    }

    return data;
}

fn createResponse(allocator: std.mem.Allocator, request: brokerRequest.BrokerRequest) !BrokerResponse {
    const header = ResponseHeader{
        .correlation_id = request.headers.correlation_id,
    };

    switch (request.headers.api_key) {
        1 => {
            // Fetch
            const body = request.body;
            var fetch_responses: []FetchTopicResponse = &[_]FetchTopicResponse{};

            if (body == .fetch) {
                const topics = body.fetch.topics;
                var responses = try allocator.alloc(FetchTopicResponse, topics.len);

                // Load cluster metadata to look up topics by UUID
                const metadata = try clusterMetadata.parseClusterMetadata(
                    allocator,
                    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
                );

                for (topics, 0..) |topic, i| {
                    // Look up the topic by UUID
                    if (metadata.findTopicById(topic.topic_id)) |topic_meta| {
                        // Topic exists - read from log file
                        const partition_count = if (topic.partition_indexes.len > 0) topic.partition_indexes.len else 1;
                        var partition_responses = try allocator.alloc(FetchPartitionResponse, partition_count);

                        for (0..partition_count) |part_idx| {
                            const partition_index: i32 = if (topic.partition_indexes.len > 0)
                                topic.partition_indexes[part_idx]
                            else
                                0;

                            // Build log file path: /tmp/kraft-combined-logs/{topic_name}-{partition}/00000000000000000000.log
                            var path_buf: [256]u8 = undefined;
                            const log_path = std.fmt.bufPrint(&path_buf, "/tmp/kraft-combined-logs/{s}-{d}/00000000000000000000.log", .{ topic_meta.name, partition_index }) catch "/tmp/kraft-combined-logs/unknown-0/00000000000000000000.log";

                            // Try to read the log file
                            const records = readLogFile(allocator, log_path);

                            partition_responses[part_idx] = FetchPartitionResponse{
                                .partition_index = partition_index,
                                .error_code = 0,
                                .records = records,
                            };
                        }

                        responses[i] = FetchTopicResponse{
                            .topic_id = topic.topic_id,
                            .partitions = partition_responses,
                        };
                    } else {
                        // Topic does not exist - return error code 100 (UNKNOWN_TOPIC_ID)
                        var partition_responses = try allocator.alloc(FetchPartitionResponse, 1);
                        partition_responses[0] = FetchPartitionResponse{
                            .partition_index = 0,
                            .error_code = 100, // UNKNOWN_TOPIC_ID
                            .records = null,
                        };

                        responses[i] = FetchTopicResponse{
                            .topic_id = topic.topic_id,
                            .partitions = partition_responses,
                        };
                    }
                }
                fetch_responses = responses;
            }

            return BrokerResponse{
                .header = header,
                .body = .{
                    .fetch = FetchResponse{
                        .throttle_time_ms = 0,
                        .error_code = 0,
                        .session_id = 0,
                        .responses = fetch_responses,
                    },
                },
            };
        },
        18 => {
            // ApiVersions
            const error_code: i16 = switch (request.headers.api_version) {
                0...4 => 0,
                else => 35,
            };

            return BrokerResponse{
                .header = header,
                .body = .{
                    .api_versions = ApiVersionsResponse{
                        .error_code = error_code,
                        .api_keys = &supported_api_keys,
                        .throttle_time_ms = 0,
                    },
                },
            };
        },
        75 => {
            // DescribeTopicPartitions
            const body = request.body;
            var topic_responses: []TopicResponse = &[_]TopicResponse{};

            if (body == .describe_topic_partitions) {
                const topic_names = body.describe_topic_partitions.topic_names;
                var responses = try allocator.alloc(TopicResponse, topic_names.len);

                // Load cluster metadata
                const metadata = try clusterMetadata.parseClusterMetadata(
                    allocator,
                    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
                );

                for (topic_names, 0..) |name, i| {
                    // Look up the topic in the cluster metadata
                    if (metadata.findTopic(name)) |topic| {
                        // Topic exists - build response with partition data
                        const partition_metadata = try metadata.findPartitionsForTopic(allocator, topic.topic_id);
                        var partition_responses = try allocator.alloc(PartitionResponse, partition_metadata.len);

                        for (partition_metadata, 0..) |partition, j| {
                            partition_responses[j] = PartitionResponse{
                                .error_code = 0,
                                .partition_index = partition.partition_index,
                                .leader_id = partition.leader_id,
                                .leader_epoch = partition.leader_epoch,
                                .replica_nodes = partition.replica_nodes,
                                .isr_nodes = partition.isr_nodes,
                            };
                        }

                        responses[i] = TopicResponse{
                            .error_code = 0, // No error
                            .name = name,
                            .topic_id = topic.topic_id,
                            .is_internal = false,
                            .partitions = partition_responses,
                            .topic_authorized_operations = 0,
                        };
                    } else {
                        // Topic does not exist
                        responses[i] = TopicResponse{
                            .error_code = 3, // UNKNOWN_TOPIC_OR_PARTITION
                            .name = name,
                            .topic_id = [_]u8{0} ** 16, // All zeros UUID
                            .is_internal = false,
                            .partitions = &[_]PartitionResponse{},
                            .topic_authorized_operations = 0,
                        };
                    }
                }

                // Sort topics alphabetically by name
                std.mem.sort(TopicResponse, responses, {}, struct {
                    fn lessThan(_: void, a: TopicResponse, b: TopicResponse) bool {
                        return std.mem.order(u8, a.name, b.name) == .lt;
                    }
                }.lessThan);

                topic_responses = responses;
            }

            return BrokerResponse{
                .header = header,
                .body = .{
                    .describe_topic_partitions = DescribeTopicPartitionsResponse{
                        .throttle_time_ms = 0,
                        .topics = topic_responses,
                    },
                },
            };
        },
        else => {
            // Default to ApiVersions with error
            return BrokerResponse{
                .header = header,
                .body = .{
                    .api_versions = ApiVersionsResponse{
                        .error_code = 35, // UNSUPPORTED_VERSION
                        .api_keys = &supported_api_keys,
                        .throttle_time_ms = 0,
                    },
                },
            };
        },
    }
}
fn write(response: *const BrokerResponse, writer: *std.Io.Writer, use_header_v1: bool) !void {
    var buf: [1024 * 1024]u8 = undefined; // 1MB buffer for records
    var fbs = std.Io.Writer.fixed(&buf);

    // Write correlation_id
    try fbs.writeInt(i32, response.header.correlation_id, .big);

    // Header v1 includes TAG_BUFFER after correlation_id
    if (use_header_v1) {
        try fbs.writeByte(0); // TAG_BUFFER (empty)
    }

    switch (response.body) {
        .api_versions => |av| {
            try fbs.writeInt(i16, av.error_code, .big);
            try fbs.writeByte(@intCast(av.api_keys.len + 1));

            for (av.api_keys) |entry| {
                try fbs.writeInt(i16, entry.api_key, .big);
                try fbs.writeInt(i16, entry.min_version, .big);
                try fbs.writeInt(i16, entry.max_version, .big);
                try fbs.writeByte(0);
            }

            try fbs.writeInt(i32, av.throttle_time_ms, .big);
            try fbs.writeByte(0);
        },
        .describe_topic_partitions => |dtp| {
            // throttle_time_ms (INT32)
            try fbs.writeInt(i32, dtp.throttle_time_ms, .big);

            // topics array (COMPACT_ARRAY: length + 1)
            try fbs.writeByte(@intCast(dtp.topics.len + 1));

            for (dtp.topics) |topic| {
                // error_code (INT16)
                try fbs.writeInt(i16, topic.error_code, .big);

                // topic_name (COMPACT_STRING: length + 1, then bytes)
                try fbs.writeByte(@intCast(topic.name.len + 1));
                try fbs.writeAll(topic.name);

                // topic_id (UUID: 16 bytes)
                try fbs.writeAll(&topic.topic_id);

                // is_internal (BOOLEAN: 1 byte)
                try fbs.writeByte(if (topic.is_internal) 1 else 0);

                // partitions array (COMPACT_ARRAY: length + 1)
                try fbs.writeByte(@intCast(topic.partitions.len + 1));

                for (topic.partitions) |partition| {
                    // error_code (INT16)
                    try fbs.writeInt(i16, partition.error_code, .big);

                    // partition_index (INT32)
                    try fbs.writeInt(i32, partition.partition_index, .big);

                    // leader_id (INT32)
                    try fbs.writeInt(i32, partition.leader_id, .big);

                    // leader_epoch (INT32)
                    try fbs.writeInt(i32, partition.leader_epoch, .big);

                    // replica_nodes (COMPACT_ARRAY of INT32)
                    try fbs.writeByte(@intCast(partition.replica_nodes.len + 1));
                    for (partition.replica_nodes) |replica| {
                        try fbs.writeInt(i32, replica, .big);
                    }

                    // isr_nodes (COMPACT_ARRAY of INT32)
                    try fbs.writeByte(@intCast(partition.isr_nodes.len + 1));
                    for (partition.isr_nodes) |isr| {
                        try fbs.writeInt(i32, isr, .big);
                    }

                    // eligible_leader_replicas (COMPACT_ARRAY: empty = 1)
                    try fbs.writeByte(1); // 0 elements

                    // last_known_elr (COMPACT_ARRAY: empty = 1)
                    try fbs.writeByte(1); // 0 elements

                    // offline_replicas (COMPACT_ARRAY: empty = 1)
                    try fbs.writeByte(1); // 0 elements

                    // TAG_BUFFER for this partition
                    try fbs.writeByte(0);
                }

                // topic_authorized_operations (INT32)
                try fbs.writeInt(i32, topic.topic_authorized_operations, .big);

                // TAG_BUFFER for this topic
                try fbs.writeByte(0);
            }

            // next_cursor (NULLABLE: -1 means null, encoded as single byte 0xff)
            try fbs.writeByte(0xff);

            // TAG_BUFFER for response body
            try fbs.writeByte(0);
        },
        .fetch => |fetch| {
            // throttle_time_ms (INT32)
            try fbs.writeInt(i32, fetch.throttle_time_ms, .big);

            // error_code (INT16)
            try fbs.writeInt(i16, fetch.error_code, .big);

            // session_id (INT32)
            try fbs.writeInt(i32, fetch.session_id, .big);

            // responses array (COMPACT_ARRAY: length + 1)
            try fbs.writeByte(@intCast(fetch.responses.len + 1));

            for (fetch.responses) |topic_response| {
                // topic_id (UUID: 16 bytes)
                try fbs.writeAll(&topic_response.topic_id);

                // partitions array (COMPACT_ARRAY: length + 1)
                try fbs.writeByte(@intCast(topic_response.partitions.len + 1));

                for (topic_response.partitions) |partition| {
                    // partition_index (INT32)
                    try fbs.writeInt(i32, partition.partition_index, .big);

                    // error_code (INT16)
                    try fbs.writeInt(i16, partition.error_code, .big);

                    // high_watermark (INT64)
                    try fbs.writeInt(i64, 0, .big);

                    // last_stable_offset (INT64)
                    try fbs.writeInt(i64, 0, .big);

                    // log_start_offset (INT64)
                    try fbs.writeInt(i64, 0, .big);

                    // aborted_transactions (COMPACT_ARRAY: empty = 1)
                    try fbs.writeByte(1);

                    // preferred_read_replica (INT32)
                    try fbs.writeInt(i32, 0, .big);

                    // records (COMPACT_NULLABLE_BYTES)
                    if (partition.records) |records| {
                        // Write length + 1 as unsigned varint
                        var len_val: u64 = records.len + 1;
                        while (len_val >= 0x80) {
                            try fbs.writeByte(@intCast((len_val & 0x7F) | 0x80));
                            len_val >>= 7;
                        }
                        try fbs.writeByte(@intCast(len_val));
                        try fbs.writeAll(records);
                    } else {
                        // null = 0
                        try fbs.writeByte(0);
                    }

                    // TAG_BUFFER for partition
                    try fbs.writeByte(0);
                }

                // TAG_BUFFER for topic
                try fbs.writeByte(0);
            }

            // TAG_BUFFER
            try fbs.writeByte(0);
        },
    }

    const written = fbs.buffered();
    try writer.writeInt(i32, @intCast(written.len), .big);
    try writer.writeAll(written);
    try writer.flush();
}

pub fn writeResponse(allocator: std.mem.Allocator, writer: *std.Io.Writer, request: brokerRequest.BrokerRequest) !void {
    const response = try createResponse(allocator, request);
    // Use header v1 for flexible APIs (Fetch v12+, DescribeTopicPartitions)
    const use_header_v1 = (request.headers.api_key == 1) or (request.headers.api_key == 75);
    try write(&response, writer, use_header_v1);
}
