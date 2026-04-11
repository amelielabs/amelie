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

typedef struct Publish Publish;

struct Publish
{
	Channel* channel;
	Buf      data;
	List     link;
};

static inline void
publish_init(Publish* self)
{
	self->channel = NULL;
	buf_init(&self->data);
	list_init(&self->link);
}

static inline void
publish_reset(Publish* self)
{
	self->channel = NULL;
	buf_reset(&self->data);
	list_init(&self->link);
}

static inline void
publish_free(Publish* self)
{
	buf_free(&self->data);
}

static inline bool
publish_empty(Publish* self)
{
	return !self->channel;
}
