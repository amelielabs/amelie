#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

always_inline hot static inline void
encode_obj(Buf* self)
{
	auto pos = buf_reserve(self, json_size_obj());
	json_write_obj(pos);
}

always_inline hot static inline void
encode_obj_end(Buf* self)
{
	auto pos = buf_reserve(self, json_size_obj_end());
	json_write_obj_end(pos);
}

always_inline hot static inline void
encode_array(Buf* self)
{
	auto pos = buf_reserve(self, json_size_array());
	json_write_array(pos);
}

always_inline hot static inline void
encode_array_end(Buf* self)
{
	auto pos = buf_reserve(self, json_size_array_end());
	json_write_array_end(pos);
}

always_inline hot static inline void
encode_raw(Buf* self, const char* pointer, int size)
{
	auto pos = buf_reserve(self, json_size_string(size));
	json_write_raw(pos, pointer, size);
}

always_inline hot static inline void
encode_buf(Buf* self, Buf* buf)
{
	auto pos = buf_reserve(self, json_size_string(buf_size(buf)));
	json_write_raw(pos, (char*)buf->start, buf_size(buf));
}

always_inline hot static inline void
encode_cstr(Buf* self, const char* pointer)
{
	encode_raw(self, pointer, strlen(pointer));
}

always_inline hot static inline void
encode_string(Buf* self, Str* string)
{
	auto pos = buf_reserve(self, json_size_string(str_size(string)));
	json_write_string(pos, string);
}

always_inline hot static inline void
encode_string32(Buf* self, int size)
{
	auto pos = buf_reserve(self, json_size_string32());
	json_write_string32(pos, size);
}

always_inline hot static inline void
encode_target(Buf* self, Str* schema, Str* name)
{
	//  schema.name
	auto size = str_size(schema) + 1 + str_size(name);
	auto size_string = json_size_string(size);
	auto pos = buf_reserve(self, size_string);
	json_write_raw(pos, NULL, size);
	memcpy(*pos, str_of(schema), str_size(schema));
	*pos += str_size(schema);
	memcpy(*pos, ".", 1);
	*pos += 1;
	memcpy(*pos, str_of(name), str_size(name));
	*pos += str_size(name);
}

always_inline hot static inline void
encode_integer(Buf* self, uint64_t value)
{
	auto pos = buf_reserve(self, json_size_integer(value));
	json_write_integer(pos, value);
}

always_inline hot static inline void
encode_bool(Buf* self, bool value)
{
	auto pos = buf_reserve(self, json_size_bool());
	json_write_bool(pos, value);
}

always_inline hot static inline void
encode_real(Buf* self, double value)
{
	auto pos = buf_reserve(self, json_size_real(value));
	json_write_real(pos, value);
}

always_inline hot static inline void
encode_null(Buf* self)
{
	auto pos = buf_reserve(self, json_size_null());
	json_write_null(pos);
}

always_inline hot static inline void
encode_uuid(Buf* self, Uuid* uuid)
{
	char uuid_sz[UUID_SZ];
	uuid_get(uuid, uuid_sz, sizeof(uuid_sz));
	encode_raw(self, uuid_sz, sizeof(uuid_sz) - 1);
}
