const std = @import("std");
const net = std.net;
const posix = std.posix;
const brokerRequest = @import("Internal/BrokerRequest.zig");
const brokerResponse = @import("Internal/BrokerResponse.zig");

pub fn main() !void {
    std.debug.print("Logs from your program will appear here!\n", .{});

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer _ = arena.reset(std.heap.ArenaAllocator.ResetMode.free_all);

    const address = try net.Address.resolveIp("127.0.0.1", 9092);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    const conn = try listener.accept();
    defer conn.stream.close();

    var reader_buffer: [4092]u8 = undefined;
    var stream_reader = conn.stream.reader(&reader_buffer);
    const stream_reader_i = stream_reader.interface();
    const parsed_request = try brokerRequest.parseRequest(arena.allocator(), stream_reader_i);
    std.debug.print("Message size: {d}\n correlationId: {d}", .{ parsed_request.message_size, parsed_request.headers.correlation_id });
    //Comment to trigger push
    var stream_buffer: [4092]u8 = undefined;
    var stream_writer = conn.stream.writer(&stream_buffer);
    try brokerResponse.writeResponse(&stream_writer.interface, parsed_request);
}
