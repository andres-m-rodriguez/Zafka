const std = @import("std");
const brokerHeader = @import("BrokerHeader.zig");

pub const BrokerRequest = struct {
    message_size: i32,
    headers: brokerHeader.BrokerHeader,
};

const ParsingState = struct {
    state: State = .Parsing_api_key,

    const State = enum { Parsing_api_key, Parsing_api_version, Parsing_correlation_id, Parsing_client_id, Done };

    pub fn next(self: *ParsingState) void {
        switch (self.state) {
            .Parsing_api_key => self.state = ParsingState.State.Parsing_api_version,
            .Parsing_api_version => self.state = ParsingState.State.Parsing_correlation_id,
            .Parsing_correlation_id => self.state = ParsingState.State.Parsing_client_id,
            .Parsing_client_id => self.state = ParsingState.State.Done,
            .Done => {},
        }
    }
};

pub const Error = std.Io.Reader.Error || error{RequestIncomplete};

pub fn parseRequest(allocator: std.mem.Allocator, reader: *std.Io.Reader) !BrokerRequest {
    var parsing_state = ParsingState{};
    std.debug.print("Parsing request", .{});

    const message_size_bytes = try reader.take(4);
    const message_size = std.mem.readInt(i32, message_size_bytes[0..4], .big);
    var brokerRequest = BrokerRequest{
        .message_size = message_size,
        .headers = brokerHeader.BrokerHeader{
            .request_api_key = 0,
            .request_api_version = 0,
            .correlation_id = 0,
            .client_id = null,
        },
    };
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);
    var consumed: usize = 0;
    while (true) {
        const data = reader.peekGreedy(1) catch |err| {
            if (buffer.items.len < message_size) {
                return Error.RequestIncomplete;
            }
            return err;
        };
        reader.toss(data.len); // Consume it
        try buffer.appendSlice(allocator, data);
        while (true) {
            switch (parsing_state.state) {
                .Parsing_api_key => {
                    const parsing_results = parseApiKey(buffer.items[consumed..]);

                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const bytes = parsing_results.complete.value;
                        const ptr: *const [2]u8 = @ptrCast(bytes.ptr);
                        brokerRequest.headers.request_api_key = std.mem.readInt(u16, ptr, .big);
                        parsing_state.next();

                    }
                },
                .Parsing_api_version => {
                    const parsing_results = parseApiVersion(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const bytes = parsing_results.complete.value;
                        const ptr: *const [2]u8 = @ptrCast(bytes.ptr);
                        brokerRequest.headers.request_api_version = std.mem.readInt(u16, ptr, .big);
                        parsing_state.next();
                    }
                },
                .Parsing_correlation_id => {
                    const parsing_results = parseCorrelationId(buffer.items[consumed..]);
                    if (parsing_results == .complete) {
                        consumed += parsing_results.complete.bytes_consumed;
                        const bytes = parsing_results.complete.value;
                        const ptr: *const [4]u8 = @ptrCast(bytes.ptr);
                        brokerRequest.headers.correlation_id = std.mem.readInt(i32, ptr, .big);
                        parsing_state.next();
                    }
                },
                .Parsing_client_id => parsing_state.next(),
                .Done => break,
            }
        }

        if (parsing_state.state == .Done) {
            break;
        }
    }

    return brokerRequest;
}

const ParseResult = union(enum) {
    complete: struct {
        value: []const u8,
        bytes_consumed: usize,
    },
    incomplete: struct {
        bytes_read: usize,
    },

    pub fn Complete(value: []const u8, bytes_consumed: usize) ParseResult {
        return ParseResult{
            .complete = .{
                .value = value,
                .bytes_consumed = bytes_consumed,
            },
        };
    }
    pub fn Incomplete(bytes_consumed: usize) ParseResult {
        return ParseResult{
            .incomplete = .{
                .bytes_read = bytes_consumed,
            },
        };
    }
};
fn parseApiKey(buffer: []const u8) ParseResult {
    std.debug.print("{d}", .{buffer.len});
    if (buffer.len < 2)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..2], 2);
}

fn parseApiVersion(buffer: []const u8) ParseResult {
    if (buffer.len < 2)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..2], 2);
}
fn parseCorrelationId(buffer: []const u8) ParseResult {
    if (buffer.len < 4)
        return ParseResult.Incomplete(buffer.len);

    return ParseResult.Complete(buffer[0..4], 4);
}
