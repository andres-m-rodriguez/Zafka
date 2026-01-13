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
    try brokerRequest.parseRequest(&stream_reader.interface_state); //Later on this will return the parsed request...maybe??

    var stream_buffer: [4092]u8 = undefined;
    const stream_writer = conn.stream.writer(&stream_buffer);
    var stream_writer_i = stream_writer.interface;
    try brokerResponse.writeResponse(&stream_writer_i);
}
