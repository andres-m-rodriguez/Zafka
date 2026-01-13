const std = @import("std");

pub fn writeResponse(writer: *std.Io.Writer) !void {
    try writer.writeInt(i32, 0, std.builtin.Endian.big);
    try writer.writeInt(i32, 7, std.builtin.Endian.big);

    try writer.flush();
}
