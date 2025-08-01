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

typedef struct ContentType ContentType;
typedef struct Content     Content;

typedef void (*ContentFn)(Content*, Columns*, Value*);

struct ContentType
{
	char*     name;
	int       name_size;
	char*     mime;
	int       mime_size;
	ContentFn fn;
};

struct Content
{
	Buf*         content;
	ContentFmt   fmt;
	ContentType* content_type;
	Local*       local;
};

void content_init(Content*);
void content_write(Content*, Str*, Columns*, Value*);
void content_write_json(Content*, Str*, Str*, Value*);
void content_write_json_buf(Content*, Str*, Str*, Buf*);
void content_write_json_error(Content*, Error*);

static inline void
content_reset(Content* self)
{
	self->content_type = NULL;
	buf_reset(self->content);
}

static inline void
content_set(Content* self, Buf* buf)
{
	self->content = buf;
	buf_reset(buf);
}

static inline void
content_set_local(Content* self, Local* local)
{
	self->local = local;
}

hot static inline void
content_ensure_limit(Content* self)
{
	auto limit = opt_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self->content) >= limit))
		error("reply limit reached");
}
