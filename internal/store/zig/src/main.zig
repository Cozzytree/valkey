const std = @import("std");
const Io = std.Io;

const zig = @import("zig");
const Allocator = std.mem.Allocator;

pub const ValkeyZigStore = struct {
    allocator: Allocator,
    map: std.StringHashMap([]u8),
};

export fn store_new() ?*ValkeyZigStore {
    const gpa = std.heap.c_allocator;

    const store = gpa.create(ValkeyZigStore) catch return null;

    store.* = .{
        .allocator = gpa,
        .map = std.StringHashMap([]u8).init(gpa),
    };
    return store;
}

export fn store_free(store: *ValkeyZigStore) void {
    const allocator = store.allocator;

    var it = store.map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        allocator.free(entry.value_ptr.*);
    }

    store.map.deinit();
    allocator.destroy(store);
}

fn dupSlice(allocator: Allocator, data: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, data.len);
    std.mem.copyForwards(u8, buf, data);
    return buf;
}

export fn store_set(
    store: *ValkeyZigStore,
    key_ptr: [*]const u8,
    key_len: usize,
    val_ptr: [*]const u8,
    val_len: usize,
) void {
    const allocator = store.allocator;

    const key = key_ptr[0..key_len];
    const val = val_ptr[0..val_len];

    const key_copy = dupSlice(allocator, key) catch return;
    const val_copy = dupSlice(allocator, val) catch return;

    const result = store.map.fetchPut(key_copy, val_copy) catch return;

    if (result) |old| {
        allocator.free(old.key);
        allocator.free(old.value);
    }
}

export fn store_get(
    store: *ValkeyZigStore,
    key_ptr: [*]const u8,
    key_len: usize,
    buf_ptr: ?[*]u8,
    buf_len: usize,
    out_len: *usize,
) i32 {
    const key = key_ptr[0..key_len];

    const value = store.map.get(key) orelse return 0;

    if (buf_ptr == null or buf_len < value.len) {
        out_len.* = value.len;
        return -1;
    }

    const buf = buf_ptr.?[0..buf_len];
    std.mem.copyForwards(u8, buf, value);

    out_len.* = value.len;
    return 1;
}

export fn store_del(
    store: *ValkeyZigStore,
    key_ptr: [*]const u8,
    key_len: usize,
) i32 {
    const key = key_ptr[0..key_len];

    if (store.map.fetchRemove(key)) |entry| {
        store.allocator.free(entry.key);
        store.allocator.free(entry.value);
        return 1;
    }

    return 0;
}

export fn store_len(store: *ValkeyZigStore) usize {
    return store.map.count();
}
