const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ValkeyZigStore = struct {
    const allocator: std.heap.DebugAllocator = .{};
    map: std.StringHashMap([]u8),
    mutex: std.Io.Mutex,

    pub fn init() ?*ValkeyZigStore {
        return .{
            .allocator = allocator,
            .map = std.StringHashMap([]u8).init(allocator),
            .mutex = std.Io.Mutex.init,
        };
    }
};

// export fn store_new() ?*ValkeyZigStore {
//     const gpa = std.heap.c_allocator;

//     const store = gpa.create(ValkeyZigStore) catch return null;
//     store.* = .{
//         .allocator = gpa,
//         .map = std.StringHashMap([]u8).init(gpa),
//         .mutex = .init,
//     };
//     return store;
// }

// export fn store_free(store: *ValkeyZigStore) void {
//     const allocator = store.allocator;

//     store.mutex.lock();
//     var it = store.map.iterator();
//     while (it.next()) |entry| {
//         allocator.free(entry.key_ptr.*);
//         allocator.free(entry.value_ptr.*);
//     }
//     store.map.deinit();
//     store.mutex.unlock();

//     allocator.destroy(store);
// }

// fn dupSlice(allocator: Allocator, data: []const u8) ![]u8 {
//     const buf = try allocator.alloc(u8, data.len);
//     @memcpy(buf, data);
//     return buf;
// }

// export fn store_set(
//     store: *ValkeyZigStore,
//     key_ptr: [*]const u8,
//     key_len: usize,
//     val_ptr: [*]const u8,
//     val_len: usize,
// ) void {
//     const allocator = store.allocator;
//     const key: []const u8 = key_ptr[0..key_len];
//     const val: []const u8 = val_ptr[0..val_len];

//     const val_copy = dupSlice(allocator, val) catch return;

//     store.mutex.lock();
//     defer store.mutex.unlock();

//     // getOrPut gives us explicit control over whether the key is new or existing.
//     // fetchPut in Zig master does NOT update the stored key on overwrite, so we
//     // cannot use it here without leaking or double-freeing the key allocation.
//     const gop = store.map.getOrPut(key) catch {
//         allocator.free(val_copy);
//         return;
//     };

//     if (gop.found_existing) {
//         // Key already present: free the old value only.
//         // The existing owned key stays in the map — no new key allocation needed.
//         allocator.free(gop.value_ptr.*);
//     } else {
//         // New key: copy it so the map owns stable memory independent of the caller.
//         const key_copy = dupSlice(allocator, key) catch {
//             allocator.free(val_copy);
//             _ = store.map.remove(key);
//             return;
//         };
//         gop.key_ptr.* = key_copy;
//     }
//     gop.value_ptr.* = val_copy;
// }

// export fn store_get(
//     store: *ValkeyZigStore,
//     key_ptr: [*]const u8,
//     key_len: usize,
//     buf_ptr: ?[*]u8,
//     buf_len: usize,
//     out_len: *usize,
// ) i32 {
//     const key: []const u8 = key_ptr[0..key_len];

//     store.mutex.lock();
//     defer store.mutex.unlock();

//     const value = store.map.get(key) orelse return 0;

//     out_len.* = value.len;
//     if (buf_ptr == null or buf_len < value.len) return -1;

//     @memcpy(buf_ptr.?[0..value.len], value);
//     return 1;
// }

// export fn store_del(
//     store: *ValkeyZigStore,
//     key_ptr: [*]const u8,
//     key_len: usize,
// ) i32 {
//     const key: []const u8 = key_ptr[0..key_len];

//     store.mutex.lock();
//     defer store.mutex.unlock();

//     if (store.map.fetchRemove(key)) |entry| {
//         store.allocator.free(entry.key);
//         store.allocator.free(entry.value);
//         return 1;
//     }
//     return 0;
// }

// export fn store_len(store: *ValkeyZigStore) usize {
//     store.mutex.lock();
//     defer store.mutex.unlock();
//     return store.map.count();
// }
