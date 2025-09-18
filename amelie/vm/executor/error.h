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
error_create(Error* error)
{
	auto self = buf_create();
	encode_obj(self);
	encode_raw(self, "msg", 3);
	encode_raw(self, error->text, error->text_len);
	encode_obj_end(self);
	return self;
}

static inline Buf*
error_create_cstr(char* msg)
{
	auto self = buf_create();
	encode_obj(self);
	encode_raw(self, "msg", 3);
	encode_cstr(self, msg);
	encode_obj_end(self);
	return self;
}

static inline void
error_buf(Buf* buf)
{
	auto pos = buf->start;
	json_read_obj(&pos);
	// msg
	json_skip(&pos);
	Str text;
	json_read_string(&pos, &text);
	error("%.*s", str_size(&text), str_of(&text));
}

static inline void
rethrow_buf(Buf* buf)
{
	auto pos = buf->start;
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
