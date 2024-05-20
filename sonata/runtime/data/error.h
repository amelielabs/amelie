#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

static inline Buf*
msg_error(Error* e)
{
	auto buf = msg_begin(MSG_ERROR);
	encode_map(buf, 2);
	encode_raw(buf, "code", 4);
	encode_integer(buf, e->code);
	encode_raw(buf, "msg", 3);
	encode_raw(buf, e->text, e->text_len);
	return msg_end(buf);
}

static inline Buf*
msg_error_as(int code, Str* msg)
{
	auto buf = msg_begin(MSG_ERROR);
	encode_map(buf, 2);
	encode_raw(buf, "code", 4);
	encode_integer(buf, code);
	encode_raw(buf, "msg", 3);
	encode_string(buf, msg);
	return msg_end(buf);
}

static inline Buf*
msg_error_rethrow(Buf* buf)
{
	uint8_t* pos = msg_of(buf)->data;
	int count;
	data_read_map(&pos, &count);
	// code
	data_skip(&pos);
	data_skip(&pos);
	// msg
	data_skip(&pos);
	Str text;
	data_read_string(&pos, &text);
	error("%.*s", str_size(&text), str_of(&text));
}
