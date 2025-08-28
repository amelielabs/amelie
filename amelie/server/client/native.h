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

typedef struct Native Native;

struct Native
{
	Msg          msg;
	RequestQueue queue;
	Str          uri;
	void*        arg;
};

static inline void
native_init(Native* self)
{
	str_init(&self->uri);
	request_queue_init(&self->queue);
	msg_init(&self->msg, MSG_NATIVE);
}

static inline void
native_free(Native* self)
{
	str_free(&self->uri);
	request_queue_free(&self->queue);
}

static inline void
native_set_uri(Native* self, char* uri)
{
	if (uri)
		str_dup_cstr(&self->uri, uri);
}

static inline void
native_attach(Native* self)
{
	request_queue_attach(&self->queue);
}

static inline void
native_detach(Native* self)
{
	request_queue_detach(&self->queue);
}
