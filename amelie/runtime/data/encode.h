#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

always_inline hot static inline void
encode_map(Buf* self)
{
	auto pos = buf_reserve(self, data_size_map());
	data_write_map(pos);
}

always_inline hot static inline void
encode_map_end(Buf* self)
{
	auto pos = buf_reserve(self, data_size_map_end());
	data_write_map_end(pos);
}

always_inline hot static inline void
encode_array(Buf* self)
{
	auto pos = buf_reserve(self, data_size_array());
	data_write_array(pos);
}

always_inline hot static inline void
encode_array_end(Buf* self)
{
	auto pos = buf_reserve(self, data_size_array_end());
	data_write_array_end(pos);
}

always_inline hot static inline void
encode_raw(Buf* self, const char* pointer, int size)
{
	auto pos = buf_reserve(self, data_size_string(size));
	data_write_raw(pos, pointer, size);
}

always_inline hot static inline void
encode_buf(Buf* self, Buf* buf)
{
	auto pos = buf_reserve(self, data_size_string(buf_size(buf)));
	data_write_raw(pos, (char*)buf->start, buf_size(buf));
}

always_inline hot static inline void
encode_cstr(Buf* self, const char* pointer)
{
	encode_raw(self, pointer, strlen(pointer));
}

always_inline hot static inline void
encode_string(Buf* self, Str* string)
{
	auto pos = buf_reserve(self, data_size_string(str_size(string)));
	data_write_string(pos, string);
}

always_inline hot static inline void
encode_string32(Buf* self, int size)
{
	auto pos = buf_reserve(self, data_size_string32());
	data_write_string32(pos, size);
}

always_inline hot static inline void
encode_integer(Buf* self, uint64_t value)
{
	auto pos = buf_reserve(self, data_size_integer(value));
	data_write_integer(pos, value);
}

always_inline hot static inline void
encode_bool(Buf* self, bool value)
{
	auto pos = buf_reserve(self, data_size_bool());
	data_write_bool(pos, value);
}

always_inline hot static inline void
encode_real(Buf* self, double value)
{
	auto pos = buf_reserve(self, data_size_real(value));
	data_write_real(pos, value);
}

always_inline hot static inline void
encode_null(Buf* self)
{
	auto pos = buf_reserve(self, data_size_null());
	data_write_null(pos);
}

always_inline hot static inline void
encode_uuid(Buf* self, Uuid* uuid)
{
	char uuid_sz[UUID_SZ];
	uuid_to_string(uuid, uuid_sz, sizeof(uuid_sz));
	encode_raw(self, uuid_sz, sizeof(uuid_sz) - 1);
}

always_inline hot static inline void
encode_interval(Buf* self, Interval* iv)
{
	auto pos = buf_reserve(self, data_size_interval(iv));
	data_write_interval(pos, iv);
}

always_inline hot static inline void
encode_timestamp(Buf* self, uint64_t value)
{
	auto pos = buf_reserve(self, data_size_timestamp(value));
	data_write_timestamp(pos, value);
}
