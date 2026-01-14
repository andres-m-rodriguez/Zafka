const std = @import("std");


pub const RequestHeader = struct {
    api_key: u16,
    api_version: u16,
    correlation_id: i32,
    client_id: ?[]const u8,
};


