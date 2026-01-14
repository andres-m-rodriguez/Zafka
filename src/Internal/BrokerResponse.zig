const std = @import("std");
const brokerRequest = @import("BrokerRequest.zig");

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

pub const BrokerResponse = struct {
    header: ResponseHeader,
    body: union(enum) {
        api_versions: ApiVersionsResponse,
    },
};


const supported_api_keys = [_]ApiKeyEntry{
    ApiKeyEntry{
        .api_key = 18,
        .min_version = 0,
        .max_version = 4,
    },
};

fn createResponse(request: brokerRequest.BrokerRequest) BrokerResponse {
    const error_code: i16 = switch (request.headers.request_api_version) {
        0...4 => 0,
        else => 35,
    };

    const api_versions_body = ApiVersionsResponse{
        .error_code = error_code,
        .api_keys = &supported_api_keys,
        .throttle_time_ms = 0,
    };

    return BrokerResponse{
        .header = ResponseHeader{
            .correlation_id = request.headers.correlation_id,
        },
        .body = .{ .api_versions = api_versions_body },
    };
}
fn write(response: *const BrokerResponse, writer: *std.Io.Writer) !void {
    var buf: [256]u8 = undefined;
    var fbs = std.Io.Writer.fixed(&buf);

    try fbs.writeInt(i32, response.header.correlation_id, .big);
    try fbs.writeByte(0);
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
    }

    const written = fbs.buffered();
    try writer.writeInt(i32, @intCast(written.len), .big);
    try writer.writeAll(written);
    try writer.flush();
}

pub fn writeResponse(request: brokerRequest.BrokerRequest, writer: *std.Io.Writer) !void {
    const response = createResponse(request);
    try write(&response, writer);
}
