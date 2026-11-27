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
	Buf list;
	int list_count;
	Iov iov;
};

static inline Gtr*
batch_at(Batch* self, int order)
{
	return ((Gtr**)self->list.start)[order];
}

static inline void
batch_init(Batch* self)
{
	self->list_count = 0;
	buf_init(&self->list);
	iov_init(&self->iov);
}

static inline void
batch_free(Batch* self)
{
	buf_free(&self->list);
	iov_free(&self->iov);
}

static inline void
batch_reset(Batch* self)
{
	self->list_count = 0;
	buf_reset(&self->list);
	iov_reset(&self->iov);
}

static inline bool
batch_empty(Batch* self)
{
	return !self->list_count;
}

static inline void
batch_add(Batch* self, Gtr* gtr)
{
	buf_write(&self->list, &gtr, sizeof(Gtr**));
	self->list_count++;
	if (access_ro(gtr->access))
		return;

	// add for wal write
	iov_add_buf(&self->iov, &gtr->write);
}

hot static inline void
batch_complete(Batch* self)
{
	// wakeup transactions
	for (auto it = 0; it < self->list_count; it++)
	{
		auto gtr = batch_at(self, it);
		event_signal(&gtr->on_commit);
	}
}
