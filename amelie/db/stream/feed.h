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

typedef struct Feed Feed;

struct Feed
{
	Str          user;
	Str          name;
	StreamCursor cursor;
};

static inline Feed*
feed_allocate(void)
{
	auto self = (Feed*)am_malloc(sizeof(Feed));
	str_init(&self->user);
	str_init(&self->name);
	stream_cursor_init(&self->cursor);
	return self;
}

static inline void
feed_free(Feed* self)
{
	str_free(&self->user);
	str_free(&self->name);
	am_free(self);
}

static inline void
feed_set_user(Feed* self, Str* value)
{
	str_free(&self->user);
	str_copy(&self->user, value);
}

static inline void
feed_set_name(Feed* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

hot static inline void
feed_collect(Feed* self, Buf* buf)
{
	for (;;)
	{
		auto event = stream_cursor_at(&self->cursor);
		if (event)
		{
			buf_format(buf, "id: {u64}\n", event->id);
			buf_write(buf, "data: ", 6);
			uint8_t* pos = event->data;
			json_export(buf, runtime()->timezone, &pos);
			buf_write(buf, "\n\n", 2);
		}
		if (! stream_cursor_next(&self->cursor))
			break;
	}
}
