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

hot static inline void
encode_string_unescape(Buf* self, Str* string)
{
	auto offset = buf_size(self);
	encode_string32(self, str_size(string));
	buf_reserve(self, str_size(string));
	unescape_str(self, string);

	// update generated string size
	auto start = self->start + offset;
	json_write_string32(&start, buf_size(self) - (offset + json_size_string32()));
}

always_inline hot static inline void
encode_base64(Buf* self, Buf* buf)
{
	auto offset = buf_size(self);
	encode_string32(self, 0);

	Str str;
	buf_str(buf, &str);
	base64url_encode(self, &str);

	// update generated string size
	auto start = self->start + offset;
	json_write_string32(&start, buf_size(self) - (offset + json_size_string32()));
}

always_inline hot static inline void
encode_target(Buf* self, Str* user, Str* name)
{
	//  user.name
	auto size = str_size(user) + 1 + str_size(name);
	auto size_string = json_size_string(size);
	auto pos = buf_reserve(self, size_string);
	json_write_raw(pos, NULL, size);
	memcpy(*pos, str_of(user), str_size(user));
	*pos += str_size(user);
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

hot static inline void
encode_date(Buf* self, int64_t value)
{
	auto offset = buf_size(self);
	encode_string32(self, 0);
	buf_reserve(self, 32);
	auto size = date_get(value, (char*)self->position, 32);
	buf_advance(self, size);
	auto pos = self->start + offset;
	json_write_string32(&pos, size);
}

hot static inline void
encode_timestamp(Buf* self, Timezone* tz, int64_t value)
{
	auto offset = buf_size(self);
	encode_string32(self, 0);
	buf_reserve(self, 64);
	auto size = timestamp_get(value, tz, (char*)self->position, 64);
	buf_advance(self, size);
	auto pos = self->start + offset;
	json_write_string32(&pos, size);
}

hot static inline void
encode_interval(Buf* self, Interval* value)
{
	auto offset = buf_size(self);
	encode_string32(self, 0);
	buf_reserve(self, 128);
	auto size = interval_get(value, (char*)self->position, 128);
	buf_advance(self, size);
	auto pos = self->start + offset;
	json_write_string32(&pos, size);
}

hot static inline void
encode_vector(Buf* self, Vector* value)
{
	encode_array(self);
	for (uint32_t i = 0; i < value->size; i++)
		encode_real(self, value->value[i]);
	encode_array_end(self);
}

always_inline hot static inline void
encode_uuid(Buf* self, Uuid* uuid)
{
	char uuid_sz[UUID_SZ];
	uuid_get(uuid, uuid_sz, sizeof(uuid_sz));
	encode_raw(self, uuid_sz, sizeof(uuid_sz) - 1);
}
