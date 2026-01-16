const std = @import("std");
const net = std.net;
const posix = std.posix;
const brokerRequest = @import("Internal/BrokerRequest.zig");
const brokerResponse = @import("Internal/BrokerResponse.zig");

fn handleClient(conn: net.Server.Connection) void {
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer _ = arena.reset(std.heap.ArenaAllocator.ResetMode.free_all);

    var reader_buffer: [4092]u8 = undefined;
    var stream_reader = conn.stream.reader(&reader_buffer);

    var stream_buffer: [4092]u8 = undefined;
    var stream_writer = conn.stream.writer(&stream_buffer);

    // Handle multiple requests on the same connection
    while (true) {
        const stream_reader_i = stream_reader.interface();
        const parsed_request = brokerRequest.parseRequest(arena.allocator(), stream_reader_i) catch |err| {
            if (err == error.EndOfStream) break;
            std.debug.print("Parse error: {any}\n", .{err});
            break;
        };
        std.debug.print("Message size: {d}\n correlationId: {d}\n", .{ parsed_request.message_size, parsed_request.headers.correlation_id });
        brokerResponse.writeResponse(arena.allocator(), &stream_writer.interface, parsed_request) catch |err| {
            std.debug.print("Write error: {any}\n", .{err});
            break;
        };
    }
}

pub fn main() !void {
    std.debug.print("Logs from your program will appear here!\n", .{});

    const address = try net.Address.resolveIp("127.0.0.1", 9092);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    // Accept and handle multiple connections concurrently
    while (true) {
        const conn = listener.accept() catch |err| {
            std.debug.print("Accept error: {any}\n", .{err});
            continue;
        };

        // Spawn a thread to handle this client
        const thread = std.Thread.spawn(.{}, handleClient, .{conn}) catch |err| {
            std.debug.print("Thread spawn error: {any}\n", .{err});
            conn.stream.close();
            continue;
        };
        thread.detach();
    }
}
