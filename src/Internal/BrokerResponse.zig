const std = @import("std");
const brokerHeader = @import("BrokerHeader.zig");
const brokerRequest = @import("./BrokerRequest.zig");
pub const BrokerResponse = struct {
    message_size: usize,
    headers: brokerHeader.BrokerHeader,
};

pub fn writeResponse(writer: *std.Io.Writer, request: brokerRequest.BrokerRequest) !void {
    try writer.writeInt(i32, 0, .big);

    try writer.writeInt(i32, request.headers.correlation_id, .big);

    const error_code: i16 = switch (request.headers.request_api_version) {
        0...4 => 0, // Valid versions
        else => 35, // UNSUPPORTED_VERSION
    };

    try writer.writeInt(i16,error_code, .big);

    try writer.flush();
}
