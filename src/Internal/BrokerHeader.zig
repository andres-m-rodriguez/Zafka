const std = @import("std");

pub const BrokerHeader = struct {
    request_api_key: u16,
    request_api_version: u16,
    correlation_id: i32,
    client_id: ?[]const u8,

    pub fn deinit(self: *BrokerHeader, allocator: std.mem.Allocator) void {
        if (self.client_id)  {
            allocator.free(self.client_id);
        }
    }
};
