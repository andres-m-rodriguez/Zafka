const std = @import("std");
const brokerHeader = @import("BrokerHeader.zig");
const brokerRequest = @import("./BrokerRequest.zig");
pub const BrokerResponse = struct {
    message_size: usize,
    headers: brokerHeader.BrokerHeader,
};

pub fn writeResponse(writer: *std.Io.Writer, request: brokerRequest.BrokerRequest) !void {
    try writer.writeInt(i32, 0, std.builtin.Endian.big);

    try writer.writeInt(i32, request.headers.correlation_id, std.builtin.Endian.big);

    try writer.flush();
}
