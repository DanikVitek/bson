const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

pub const ParseError = error{
    InvalidDocumentSize,
    InvalidStringSize,
    InvalidBinarySize,
    InvalidArrayIndex,
    InvalidBooleanValue,
} || BinarySubtype.Error || ElementType.Error;

pub fn parse(input: []const u8, allocator: Allocator) (ParseError || Allocator.Error)!Document {
    var input_ = input;
    if (input_.len < 5 or input_[input_.len - 1] != 0) {
        return ParseError.InvalidDocumentSize;
    }

    const doc_size = std.mem.readIntSliceLittle(i32, input_);
    if (doc_size < 0 or @as(usize, @intCast(doc_size)) != input_.len) {
        return ParseError.InvalidDocumentSize;
    }

    input_ = input_[4 .. input_.len - 1];

    var e_list = ArrayList(Element).init(allocator);
    errdefer e_list.deinit();

    while (input_.len > 0) {
        const et = try ElementType.tryFromByte(input_[0]);
        input_ = input_[1..];

        const e_name_end = indexOfSentinel(u8, 0, input_);
        const e_name = input_[0..e_name_end];
        input_ = input_[e_name_end + 1 ..];

        switch (et) {
            ElementType.double => {
                const value: f64 = @bitCast(std.mem.readIntSliceLittle(u64, input_));
                input_ = input_[8..];

                try e_list.append(Element{ .double = .{ .e_name = e_name, .value = value } });
            },
            ElementType.string, ElementType.js_code, ElementType.symbol => {
                const string_bytes = std.mem.readIntSliceLittle(i32, input_);
                input_ = input_[4..];

                if (string_bytes < 0) {
                    return ParseError.InvalidStringSize;
                }
                const string_bytes_u: usize = @intCast(string_bytes);
                if (string_bytes_u > input_.len or input_[string_bytes_u - 1] != 0) {
                    return ParseError.InvalidStringSize;
                }

                const value = input_[0 .. string_bytes_u - 1];
                input_ = input_[string_bytes_u..];

                const el = switch (et) {
                    ElementType.string => Element{ .string = .{ .e_name = e_name, .value = value } },
                    ElementType.js_code => Element{ .js_code = .{ .e_name = e_name, .value = value } },
                    ElementType.symbol => Element{ .symbol = .{ .e_name = e_name, .value = value } },
                    else => unreachable,
                };
                try e_list.append(el);
            },
            ElementType.document => {
                const value = try parse(input_, allocator);
                input_ = input_[@intCast(value.size)..];

                try e_list.append(Element{ .document = .{ .e_name = e_name, .value = value } });
            },
            ElementType.array => {
                const value = try parse(input_, allocator);
                input_ = input_[@intCast(value.size)..];

                for (0.., value.e_list) |i, *element| {
                    if ((std.fmt.parseInt(usize, element.name(), 10) catch return ParseError.InvalidArrayIndex) != i) {
                        return ParseError.InvalidArrayIndex;
                    }
                }

                try e_list.append(Element{ .array = .{ .e_name = e_name, .value = value } });
            },
            ElementType.binary => {
                const binary_size = std.mem.readIntSliceLittle(i32, input_);
                input_ = input_[4..];

                if (binary_size < 0 or @as(usize, @intCast(binary_size)) > input_.len) {
                    return ParseError.InvalidBinarySize;
                }

                const binary_subtype = try BinarySubtype.tryFromByte(input_[0]);
                input_ = input_[1..];

                const value = input_[0..@intCast(binary_size)];
                input_ = input_[@intCast(binary_size)..];

                try e_list.append(Element{ .binary = .{ .e_name = e_name, .value = .{ .subtype = binary_subtype, .data = value } } });
            },
            ElementType.undefined => try e_list.append(Element{ .undefined = .{ .e_name = e_name } }),
            ElementType.object_id => {
                const value: *const [12]u8 = input[0..12];
                input_ = input_[12..];

                try e_list.append(Element{ .object_id = .{ .e_name = e_name, .value = value } });
            },
            ElementType.boolean => {
                const value = switch (input_[0]) {
                    0 => false,
                    1 => true,
                    else => return ParseError.InvalidBooleanValue,
                };
                input_ = input_[1..];

                try e_list.append(Element{ .boolean = .{ .e_name = e_name, .value = value } });
            },
            ElementType.utc_datetime => {
                const value = std.mem.readIntSliceLittle(i64, input_);
                input_ = input_[8..];

                try e_list.append(Element{ .utc_datetime = .{ .e_name = e_name, .value = value } });
            },
            ElementType.null => try e_list.append(Element{ .null = .{ .e_name = e_name } }),
            ElementType.regex => {
                const pattern_end = indexOfSentinel(u8, 0, input_);
                const pattern = input_[0..pattern_end];
                input_ = input_[pattern_end + 1 ..];

                const options_end = indexOfSentinel(u8, 0, input_);
                const options = input_[0..options_end];
                input_ = input_[options_end + 1 ..];

                try e_list.append(Element{ .regex = .{ .e_name = e_name, .value = .{ .pattern = pattern, .options = options } } });
            },
            ElementType.db_pointer => {
                const ns_bytes = std.mem.readIntSliceLittle(i32, input_);
                input_ = input_[4..];

                if (ns_bytes < 0 or @as(usize, @intCast(ns_bytes)) > input_.len or input_[@as(usize, @intCast(ns_bytes)) - 1] != 0) {
                    return ParseError.InvalidStringSize;
                }

                const ns = input_[0..@intCast(ns_bytes)];
                input_ = input_[@as(usize, @intCast(ns_bytes)) + 1 ..];

                const oid: *const [12]u8 = input_[0..12];
                input_ = input_[12..];

                try e_list.append(Element{ .db_pointer = .{ .e_name = e_name, .value = .{ .ns = ns, .oid = oid } } });
            },
            ElementType.js_code_with_scope => {
                const e_bytes = std.mem.readIntSliceLittle(i32, input_);
                input_ = input_[4..];

                if (e_bytes < 0 or @as(usize, @intCast(e_bytes)) > input_.len) {
                    return ParseError.InvalidDocumentSize;
                }

                const code_bytes = std.mem.readIntSliceLittle(i32, input_);
                input_ = input_[4..];

                if (code_bytes < 0) {
                    return ParseError.InvalidStringSize;
                }
                const code_bytes_u: usize = @intCast(code_bytes);
                if (code_bytes_u > input_.len or input_[code_bytes_u - 1] != 0) {
                    return ParseError.InvalidStringSize;
                }

                const code = input_[0 .. code_bytes_u - 1];
                input_ = input_[code_bytes_u..];

                const scope = try parse(input_, allocator);
                input_ = input_[@intCast(scope.size)..];

                try e_list.append(Element{ .js_code_with_scope = .{
                    .e_name = e_name,
                    .value = .{ .e_bytes = e_bytes, .code = code, .scope = scope },
                } });
            },
            ElementType.int32 => {
                const value = std.mem.readIntSliceLittle(i32, input_);
                input_ = input_[4..];

                try e_list.append(Element{ .int32 = .{ .e_name = e_name, .value = value } });
            },
            ElementType.timestamp => {
                const value = std.mem.readIntSliceLittle(u64, input_);
                input_ = input_[8..];

                try e_list.append(Element{ .timestamp = .{ .e_name = e_name, .value = value } });
            },
            ElementType.int64 => {
                const value = std.mem.readIntSliceLittle(i64, input_);
                input_ = input_[8..];

                try e_list.append(Element{ .int64 = .{ .e_name = e_name, .value = value } });
            },
            ElementType.decimal128 => {
                const value: f128 = @bitCast(std.mem.readIntSliceLittle(u128, input_));
                input_ = input_[16..];

                try e_list.append(Element{ .decimal128 = .{ .e_name = e_name, .value = value } });
            },
            ElementType.min_key => try e_list.append(Element{ .min_key = .{ .e_name = e_name } }),
            ElementType.max_key => try e_list.append(Element{ .max_key = .{ .e_name = e_name } }),
        }
    }

    return .{
        .size = doc_size,
        .e_list = try e_list.toOwnedSlice(),
    };
}

fn indexOfSentinel(comptime T: type, comptime sentinel: T, slice: []const T) usize {
    var i: usize = 0;
    while (slice[i] != sentinel) {
        i += 1;
    }
    return i;
}

pub const Document = struct {
    size: i32,
    e_list: []const Element,

    pub fn free(self: Document, allocator: Allocator) void {
        defer allocator.free(self.e_list);
        for (self.e_list) |e| {
            switch (e) {
                .double => {},
                .string => {},
                .document => |document| document.value.free(allocator),
                .array => |array| array.value.free(allocator),
                .binary => {},
                .undefined => {},
                .object_id => {},
                .boolean => {},
                .utc_datetime => {},
                .null => {},
                .regex => {},
                .db_pointer => {},
                .js_code => {},
                .symbol => {},
                .js_code_with_scope => |code_w_s| code_w_s.value.scope.free(allocator),
                .int32 => {},
                .timestamp => {},
                .int64 => {},
                .decimal128 => {},
                .min_key => {},
                .max_key => {},
            }
        }
    }
};

pub const Binary = struct {
    subtype: BinarySubtype,
    data: []const u8,
};
pub const BinarySubtype = struct {
    value: u8,

    pub const generic: BinarySubtype = .{ .value = 0 };
    pub const function: BinarySubtype = .{ .value = 1 };
    pub const binary_old: BinarySubtype = .{ .value = 2 };
    pub const uuid_old: BinarySubtype = .{ .value = 3 };
    pub const uuid: BinarySubtype = .{ .value = 4 };
    pub const md5: BinarySubtype = .{ .value = 5 };
    pub const encrypted_bson_value: BinarySubtype = .{ .value = 6 };
    pub const compressed_bson_column: BinarySubtype = .{ .value = 7 };
    pub const sensitive: BinarySubtype = .{ .value = 8 };

    pub const Error = error{InvalidSubtype};

    pub fn tryFromByte(value: u8) Error!BinarySubtype {
        return switch (value) {
            0...8, 128...255 => BinarySubtype{ .value = value },
            else => Error.InvalidSubtype,
        };
    }

    pub fn isUserDefined(self: BinarySubtype) bool {
        return self.value >= 128;
    }
};
pub const Regex = struct {
    pattern: []const u8,
    options: []const u8,
};
pub const DBPointer = struct {
    ns: []const u8,
    oid: *const [12]u8,
};
/// deprecated
pub const CodeWithScope = struct {
    /// length in bytes of the entire code_with_scope value
    e_bytes: i32,
    /// the JavaScript code
    code: []const u8,
    /// Mapping from identifiers to values, representing the scope in which the code should be evaluated
    scope: Document,
};

pub const ElementType = enum(i8) {
    double = 1,
    string = 2,
    document = 3,
    array = 4,
    binary = 5,
    /// deprecated
    undefined = 6,
    object_id = 7,
    boolean = 8,
    utc_datetime = 9,
    null = 10,
    regex = 11,
    /// deprecated
    db_pointer = 12,
    js_code = 13,
    /// deprecated
    symbol = 14,
    /// deprecated
    js_code_with_scope = 15,
    int32 = 16,
    timestamp = 17,
    int64 = 18,
    decimal128 = 19,
    min_key = -1,
    max_key = 127,

    pub const Error = error{InvalidElementType};

    fn tryFromByte(value: u8) Error!ElementType {
        return switch (@as(i8, @bitCast(value))) {
            -1, 127, 1...19 => |et| @enumFromInt(et),
            else => error.InvalidElementType,
        };
    }
};

pub const Element = union(ElementType) {
    double: Field(f64),
    string: Field([]const u8),
    document: Field(Document),
    array: Field(Document),
    binary: Field(Binary),
    /// deprecated
    undefined: Field(void),
    object_id: Field(*const [12]u8),
    boolean: Field(bool),
    utc_datetime: Field(i64),
    null: Field(void),
    regex: Field(Regex),
    /// deprecated
    db_pointer: Field(DBPointer),
    js_code: Field([]const u8),
    symbol: Field([]const u8),
    /// deprecated
    js_code_with_scope: Field(CodeWithScope),
    int32: Field(i32),
    timestamp: Field(u64),
    int64: Field(i64),
    decimal128: Field(f128),
    min_key: Field(void),
    max_key: Field(void),

    fn Field(comptime T: type) type {
        return switch (@typeInfo(T)) {
            .Void => struct { e_name: []const u8 },
            else => struct { e_name: []const u8, value: T },
        };
    }

    fn name(self: *const Element) []const u8 {
        return switch (self.*) {
            .double => |e| e.e_name,
            .string => |e| e.e_name,
            .document => |e| e.e_name,
            .array => |e| e.e_name,
            .binary => |e| e.e_name,
            .undefined => |e| e.e_name,
            .object_id => |e| e.e_name,
            .boolean => |e| e.e_name,
            .utc_datetime => |e| e.e_name,
            .null => |e| e.e_name,
            .regex => |e| e.e_name,
            .db_pointer => |e| e.e_name,
            .js_code => |e| e.e_name,
            .symbol => |e| e.e_name,
            .js_code_with_scope => |e| e.e_name,
            .int32 => |e| e.e_name,
            .timestamp => |e| e.e_name,
            .int64 => |e| e.e_name,
            .decimal128 => |e| e.e_name,
            .min_key => |e| e.e_name,
            .max_key => |e| e.e_name,
        };
    }
};

test "parse returns error if input len is less than 5" {
    inline for (0..5) |len| {
        const input: [len]u8 = undefined;
        const result = parse(&input, testing.allocator);
        try testing.expectError(ParseError.InvalidDocumentSize, result);
    }
}

test "parse returns error if input is not null terminated" {
    var input: [5]u8 = undefined;
    for (1..256) |b| {
        input[4] = @intCast(b);
        const result = parse(&input, testing.allocator);
        try testing.expectError(ParseError.InvalidDocumentSize, result);
    }
}

test "successfully parses {\"hello\":\"world\"} object" {
    const input = "\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00";

    const result = try parse(input, testing.allocator);
    defer result.free(testing.allocator);

    try testing.expectEqual(@as(i32, 22), result.size);
    try testing.expectEqual(@as(usize, 1), result.e_list.len);
    try testing.expectEqualStrings("hello", result.e_list[0].name());
    try testing.expectEqualStrings("world", result.e_list[0].string.value);
}

test "successfully parses {\"BSON\": [\"awesome\", 5.05, 1986]}" {
    const input = "\x31\x00\x00\x00\x04BSON\x00\x26\x00\x00\x00\x02\x30\x00\x08\x00\x00\x00awesome\x00\x01\x31\x00\x33\x33\x33\x33\x33\x33\x14\x40\x10\x32\x00\xc2\x07\x00\x00\x00\x00";

    const result = try parse(input, testing.allocator);
    defer result.free(testing.allocator);

    try testing.expectEqual(@as(i32, 49), result.size);
    try testing.expectEqual(@as(usize, 1), result.e_list.len);
    try testing.expectEqualStrings("BSON", result.e_list[0].name());
    try testing.expectEqualStrings("awesome", result.e_list[0].array.value.e_list[0].string.value);
    try testing.expectEqual(@as(f64, 5.05), result.e_list[0].array.value.e_list[1].double.value);
    try testing.expectEqual(@as(i32, 1986), result.e_list[0].array.value.e_list[2].int32.value);
}
