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
	RequestQueue queue;
};

static inline void
native_init(Native* self)
{
	request_queue_init(&self->queue);
}

static inline void
native_free(Native* self)
{
	request_queue_free(&self->queue);
}
