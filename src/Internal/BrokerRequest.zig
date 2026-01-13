const std = @import("std");

pub fn parseRequest(reader: *std.Io.Reader) !void {
    const request_line = try reader.takeDelimiterInclusive('\n');

    _ = request_line; // for now do nothing with the request line itself
}
