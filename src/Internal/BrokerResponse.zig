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

pub const BrokerResponse = struct {
    header: ResponseHeader,
    body: union(enum) {
        api_versions: ApiVersionsResponse,
        describe_topic_partitions: DescribeTopicPartitionsResponse,
    },
};


const supported_api_keys = [_]ApiKeyEntry{
    ApiKeyEntry{
        .api_key = 18,
        .min_version = 0,
        .max_version = 4,
    },ApiKeyEntry{
        .api_key = 75,
        .min_version = 0,
        .max_version = 0,
    },
};

fn createResponse(allocator: std.mem.Allocator, request: brokerRequest.BrokerRequest) !BrokerResponse {
    const header = ResponseHeader{
        .correlation_id = request.headers.correlation_id,
    };

    switch (request.headers.api_key) {
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
    var buf: [1024]u8 = undefined;
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
    }

    const written = fbs.buffered();
    try writer.writeInt(i32, @intCast(written.len), .big);
    try writer.writeAll(written);
    try writer.flush();
}

pub fn writeResponse(allocator: std.mem.Allocator, writer: *std.Io.Writer, request: brokerRequest.BrokerRequest) !void {
    const response = try createResponse(allocator, request);
    // Use header v1 for DescribeTopicPartitions (api_key 75)
    const use_header_v1 = request.headers.api_key == 75;
    try write(&response, writer, use_header_v1);
}
