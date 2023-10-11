#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline Buf*
make_bool(bool value)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_bool(buf, value);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_float(float value)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_float(buf, value);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_integer(int64_t value)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_integer(buf, value);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_string(Str* string)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_string(buf, string);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_null(void)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_null(buf);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_error(Error* e)
{
	auto buf = msg_create(MSG_ERROR);
	encode_map(buf, 2);
	encode_raw(buf, "code", 4);
	encode_integer(buf, e->code);
	encode_raw(buf, "msg", 3);
	encode_raw(buf, e->text, e->text_len);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_error_as(int code, Str* msg)
{
	auto buf = msg_create(MSG_ERROR);
	encode_map(buf, 2);
	encode_raw(buf, "code", 4);
	encode_integer(buf, code);
	encode_raw(buf, "msg", 3);
	encode_string(buf, msg);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_copy(uint8_t* data)
{
	uint8_t* start = data;
	uint8_t* end = start;
	data_skip(&end);
	int size = end - start;
	auto buf = msg_create(MSG_OBJECT);
	buf_write(buf, data, size);
	msg_end(buf);
	return buf;
}

static inline Buf*
make_copy_buf(Buf* buf)
{
	auto copy = msg_create(MSG_OBJECT);
	buf_write(copy, buf->start, buf_size(buf));
	msg_end(buf);
	return copy;
}
