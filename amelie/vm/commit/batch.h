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

typedef struct Batch Batch;

struct Batch
{
	Buf    list;
	int    count;
	Writes writes;
};

static inline Gtr*
batch_at(Batch* self, int order)
{
	return ((Gtr**)self->list.start)[order];
}

static inline void
batch_init(Batch* self)
{
	self->count = 0;
	buf_init(&self->list);
	writes_init(&self->writes);
}

static inline void
batch_free(Batch* self)
{
	buf_free(&self->list);
}

static inline void
batch_reset(Batch* self)
{
	self->count = 0;
	buf_reset(&self->list);
	writes_reset(&self->writes);
}

static inline bool
batch_empty(Batch* self)
{
	return !self->count;
}

static inline void
batch_add(Batch* self, Gtr* gtr)
{
	buf_write(&self->list, &gtr, sizeof(Gtr**));
	self->count++;
	if (access_ro(gtr->access))
		return;

	// add write
	auto write = write_of(&gtr->write);
	writes_add(&self->writes, write);
}

hot static inline void
batch_complete(Batch* self)
{
	// wakeup transactions
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);
		event_signal(&gtr->on_commit);
	}
}
