const std = @import("std");

pub const TopicMetadata = struct {
    name: []const u8,
    topic_id: [16]u8,
};

pub const PartitionMetadata = struct {
    partition_index: i32,
    topic_id: [16]u8,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: []const i32,
    isr_nodes: []const i32,
};

pub const ClusterMetadata = struct {
    topics: []const TopicMetadata,
    partitions: []const PartitionMetadata,

    pub fn findTopic(self: *const ClusterMetadata, name: []const u8) ?*const TopicMetadata {
        for (self.topics) |*topic| {
            if (std.mem.eql(u8, topic.name, name)) {
                return topic;
            }
        }
        return null;
    }

    pub fn findTopicById(self: *const ClusterMetadata, topic_id: [16]u8) ?*const TopicMetadata {
        for (self.topics) |*topic| {
            if (std.mem.eql(u8, &topic.topic_id, &topic_id)) {
                return topic;
            }
        }
        return null;
    }

    pub fn findPartitionsForTopic(self: *const ClusterMetadata, allocator: std.mem.Allocator, topic_id: [16]u8) ![]const PartitionMetadata {
        var count: usize = 0;
        for (self.partitions) |partition| {
            if (std.mem.eql(u8, &partition.topic_id, &topic_id)) {
                count += 1;
            }
        }

        if (count == 0) {
            return &[_]PartitionMetadata{};
        }

        var result = try allocator.alloc(PartitionMetadata, count);
        var idx: usize = 0;
        for (self.partitions) |partition| {
            if (std.mem.eql(u8, &partition.topic_id, &topic_id)) {
                result[idx] = partition;
                idx += 1;
            }
        }
        return result;
    }
};

// Read a variable-length integer (varint) from a slice
fn readVarint(data: []const u8) struct { value: i64, bytes_consumed: usize } {
    var result: i64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;

    while (i < data.len) {
        const byte = data[i];
        result |= @as(i64, byte & 0x7F) << shift;
        i += 1;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    // Zigzag decode for signed varints
    const decoded = (result >> 1) ^ (-(result & 1));
    return .{ .value = decoded, .bytes_consumed = i };
}

// Read unsigned varint (for lengths)
fn readUnsignedVarint(data: []const u8) struct { value: u64, bytes_consumed: usize } {
    var result: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;

    while (i < data.len) {
        const byte = data[i];
        result |= @as(u64, byte & 0x7F) << shift;
        i += 1;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    return .{ .value = result, .bytes_consumed = i };
}

pub fn parseClusterMetadata(allocator: std.mem.Allocator, path: []const u8) !ClusterMetadata {
    const file = std.fs.openFileAbsolute(path, .{}) catch |err| {
        std.debug.print("Failed to open cluster metadata file: {any}\n", .{err});
        return ClusterMetadata{ .topics = &[_]TopicMetadata{}, .partitions = &[_]PartitionMetadata{} };
    };
    defer file.close();

    const data = file.readToEndAlloc(allocator, 1024 * 1024) catch |err| {
        std.debug.print("Failed to read cluster metadata file: {any}\n", .{err});
        return ClusterMetadata{ .topics = &[_]TopicMetadata{}, .partitions = &[_]PartitionMetadata{} };
    };

    var topics = std.ArrayList(TopicMetadata){};
    var partitions = std.ArrayList(PartitionMetadata){};

    var offset: usize = 0;

    // Parse record batches
    while (offset + 61 < data.len) { // Minimum batch header size
        // Record batch header:
        // base_offset: i64 (8 bytes)
        // batch_length: i32 (4 bytes)
        // partition_leader_epoch: i32 (4 bytes)
        // magic: i8 (1 byte)
        // crc: i32 (4 bytes)
        // attributes: i16 (2 bytes)
        // last_offset_delta: i32 (4 bytes)
        // base_timestamp: i64 (8 bytes)
        // max_timestamp: i64 (8 bytes)
        // producer_id: i64 (8 bytes)
        // producer_epoch: i16 (2 bytes)
        // base_sequence: i32 (4 bytes)
        // records_count: i32 (4 bytes)
        // Total: 61 bytes

        const batch_length = std.mem.readInt(i32, data[offset + 8 ..][0..4], .big);
        if (batch_length <= 0) break;

        const records_count = std.mem.readInt(i32, data[offset + 57 ..][0..4], .big);

        // Records start after the batch header (61 bytes from start, but we need to account for base_offset)
        var record_offset = offset + 61;
        const batch_end = offset + 12 + @as(usize, @intCast(batch_length)); // base_offset(8) + batch_length(4) + batch_length value

        for (0..@intCast(records_count)) |_| {
            if (record_offset >= batch_end or record_offset >= data.len) break;

            // Record format:
            // length (varint)
            // attributes (1 byte)
            // timestamp_delta (varint)
            // offset_delta (varint)
            // key_length (varint)
            // key (key_length bytes)
            // value_length (varint)
            // value (value_length bytes)
            // headers_count (varint)

            const length_result = readVarint(data[record_offset..]);
            record_offset += length_result.bytes_consumed;
            const record_length = @as(usize, @intCast(length_result.value));

            if (record_offset + record_length > data.len) break;

            const record_data = data[record_offset .. record_offset + record_length];
            var rec_pos: usize = 0;

            // Skip attributes (1 byte)
            rec_pos += 1;

            // Skip timestamp_delta (varint)
            const ts_result = readVarint(record_data[rec_pos..]);
            rec_pos += ts_result.bytes_consumed;

            // Skip offset_delta (varint)
            const od_result = readVarint(record_data[rec_pos..]);
            rec_pos += od_result.bytes_consumed;

            // Key length (varint)
            const key_len_result = readVarint(record_data[rec_pos..]);
            rec_pos += key_len_result.bytes_consumed;
            const key_length = key_len_result.value;

            // Skip key
            if (key_length > 0) {
                rec_pos += @intCast(key_length);
            }

            // Value length (varint)
            const val_len_result = readVarint(record_data[rec_pos..]);
            rec_pos += val_len_result.bytes_consumed;
            const value_length = val_len_result.value;

            if (value_length > 0 and rec_pos + @as(usize, @intCast(value_length)) <= record_data.len) {
                const value_data = record_data[rec_pos .. rec_pos + @as(usize, @intCast(value_length))];

                // Value format for cluster metadata:
                // frame_version: u8 (1 byte) - should be 1
                // record_type: u8 (1 byte)
                // record_version: u8 (1 byte)
                // ... fields based on record_type

                if (value_data.len >= 3) {
                    const record_type = value_data[1];
                    const record_version = value_data[2];
                    _ = record_version;

                    if (record_type == 2) {
                        // TopicRecord
                        if (parseTopicRecord(allocator, value_data[3..])) |topic| {
                            topics.append(allocator, topic) catch {};
                        }
                    } else if (record_type == 3) {
                        // PartitionRecord
                        if (parsePartitionRecord(allocator, value_data[3..])) |partition| {
                            partitions.append(allocator, partition) catch {};
                        }
                    }
                }
            }

            record_offset += record_length;
        }

        offset = batch_end;
    }

    return ClusterMetadata{
        .topics = topics.toOwnedSlice(allocator) catch &[_]TopicMetadata{},
        .partitions = partitions.toOwnedSlice(allocator) catch &[_]PartitionMetadata{},
    };
}

fn parseTopicRecord(allocator: std.mem.Allocator, data: []const u8) ?TopicMetadata {
    // TopicRecord format (after frame header):
    // name: COMPACT_STRING
    // topic_id: UUID (16 bytes)
    // ...other fields

    if (data.len < 2) return null;

    var pos: usize = 0;

    // Topic name (COMPACT_STRING: length byte where actual = byte - 1)
    const name_len_byte = data[pos];
    pos += 1;
    const name_len: usize = if (name_len_byte > 0) name_len_byte - 1 else 0;

    if (pos + name_len + 16 > data.len) return null;

    const name = allocator.alloc(u8, name_len) catch return null;
    @memcpy(name, data[pos .. pos + name_len]);
    pos += name_len;

    // Topic ID (UUID: 16 bytes)
    var topic_id: [16]u8 = undefined;
    @memcpy(&topic_id, data[pos .. pos + 16]);

    return TopicMetadata{
        .name = name,
        .topic_id = topic_id,
    };
}

fn parsePartitionRecord(allocator: std.mem.Allocator, data: []const u8) ?PartitionMetadata {
    // PartitionRecord format (after frame header):
    // partition_id: INT32
    // topic_id: UUID (16 bytes)
    // replicas: COMPACT_ARRAY of INT32
    // isr: COMPACT_ARRAY of INT32
    // removing_replicas: COMPACT_ARRAY of INT32
    // adding_replicas: COMPACT_ARRAY of INT32
    // leader: INT32
    // leader_epoch: INT32
    // ...other fields

    if (data.len < 20) return null; // At least partition_id(4) + topic_id(16)

    var pos: usize = 0;

    // Partition ID (INT32)
    const partition_index = std.mem.readInt(i32, data[pos..][0..4], .big);
    pos += 4;

    // Topic ID (UUID: 16 bytes)
    var topic_id: [16]u8 = undefined;
    @memcpy(&topic_id, data[pos .. pos + 16]);
    pos += 16;

    // Replicas (COMPACT_ARRAY of INT32)
    if (pos >= data.len) return null;
    const replicas_len_byte = data[pos];
    pos += 1;
    const replicas_len: usize = if (replicas_len_byte > 0) replicas_len_byte - 1 else 0;

    var replica_nodes = allocator.alloc(i32, replicas_len) catch return null;
    for (0..replicas_len) |i| {
        if (pos + 4 > data.len) return null;
        replica_nodes[i] = std.mem.readInt(i32, data[pos..][0..4], .big);
        pos += 4;
    }

    // ISR (COMPACT_ARRAY of INT32)
    if (pos >= data.len) return null;
    const isr_len_byte = data[pos];
    pos += 1;
    const isr_len: usize = if (isr_len_byte > 0) isr_len_byte - 1 else 0;

    var isr_nodes = allocator.alloc(i32, isr_len) catch return null;
    for (0..isr_len) |i| {
        if (pos + 4 > data.len) return null;
        isr_nodes[i] = std.mem.readInt(i32, data[pos..][0..4], .big);
        pos += 4;
    }

    // Skip removing_replicas
    if (pos >= data.len) return null;
    const removing_len_byte = data[pos];
    pos += 1;
    const removing_len: usize = if (removing_len_byte > 0) removing_len_byte - 1 else 0;
    pos += removing_len * 4;

    // Skip adding_replicas
    if (pos >= data.len) return null;
    const adding_len_byte = data[pos];
    pos += 1;
    const adding_len: usize = if (adding_len_byte > 0) adding_len_byte - 1 else 0;
    pos += adding_len * 4;

    // Leader ID (INT32)
    if (pos + 4 > data.len) return null;
    const leader_id = std.mem.readInt(i32, data[pos..][0..4], .big);
    pos += 4;

    // Leader epoch (INT32)
    if (pos + 4 > data.len) return null;
    const leader_epoch = std.mem.readInt(i32, data[pos..][0..4], .big);

    return PartitionMetadata{
        .partition_index = partition_index,
        .topic_id = topic_id,
        .leader_id = leader_id,
        .leader_epoch = leader_epoch,
        .replica_nodes = replica_nodes,
        .isr_nodes = isr_nodes,
    };
}
