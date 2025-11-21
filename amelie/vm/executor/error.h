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
	encode_buf(self, &error->text);
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

	error_set(&am_self()->error,
	          source_file,
	          source_function,
	          source_line,
	          0, "%.*s", str_size(&text),
	          str_of(&text));
	rethrow();
}
