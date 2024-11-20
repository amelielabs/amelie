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

static inline Buf*
msg_error(Error* e)
{
	auto buf = msg_create(MSG_ERROR);
	encode_obj(buf);
	encode_raw(buf, "msg", 3);
	encode_raw(buf, e->text, e->text_len);
	encode_obj_end(buf);
	msg_end(buf);
	return buf;
}

static inline Buf*
msg_error_as(Str* msg)
{
	auto buf = msg_create(MSG_ERROR);
	encode_obj(buf);
	encode_raw(buf, "msg", 3);
	encode_string(buf, msg);
	encode_obj_end(buf);
	msg_end(buf);
	return buf;
}

static inline void
msg_error_throw(Buf* buf)
{
	uint8_t* pos = msg_of(buf)->data;
	json_read_obj(&pos);
	// msg
	json_skip(&pos);
	Str text;
	json_read_string(&pos, &text);
	error("%.*s", str_size(&text), str_of(&text));
}

static inline void
msg_error_rethrow(Buf* buf)
{
	uint8_t* pos = msg_of(buf)->data;
	json_read_obj(&pos);
	// msg
	json_skip(&pos);
	Str text;
	json_read_string(&pos, &text);

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
