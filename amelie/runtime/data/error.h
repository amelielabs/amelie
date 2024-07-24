#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline Buf*
msg_error(Error* e)
{
	auto buf = msg_begin(MSG_ERROR);
	encode_map(buf);
	encode_raw(buf, "code", 4);
	encode_integer(buf, e->code);
	encode_raw(buf, "msg", 3);
	encode_raw(buf, e->text, e->text_len);
	encode_map_end(buf);
	return msg_end(buf);
}

static inline Buf*
msg_error_as(int code, Str* msg)
{
	auto buf = msg_begin(MSG_ERROR);
	encode_map(buf);
	encode_raw(buf, "code", 4);
	encode_integer(buf, code);
	encode_raw(buf, "msg", 3);
	encode_string(buf, msg);
	encode_map_end(buf);
	return msg_end(buf);
}

static inline void
msg_error_throw(Buf* buf)
{
	uint8_t* pos = msg_of(buf)->data;
	data_read_map(&pos);
	// code
	data_skip(&pos);
	data_skip(&pos);
	// msg
	data_skip(&pos);
	Str text;
	data_read_string(&pos, &text);
	error("%.*s", str_size(&text), str_of(&text));
}

static inline void
msg_error_rethrow(Buf* buf)
{
	uint8_t* pos = msg_of(buf)->data;
	data_read_map(&pos);
	// code
	data_skip(&pos);
	data_skip(&pos);
	// msg
	data_skip(&pos);
	Str text;
	data_read_string(&pos, &text);

	char sz[1024];
	snprintf(sz, sizeof(sz), "%.*s", str_size(&text), str_of(&text));

	auto self = am_self();
	error_throw(&self->error,
	            &self->exception_mgr,
	            source_file,
	            source_function,
	            source_line,
	            ERROR, sz);
}
