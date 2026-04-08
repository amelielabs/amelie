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

typedef struct StreamCursor StreamCursor;

struct StreamCursor
{
	uint64_t    lsn;
	uint32_t    offset;
	StreamPage* page;
	Stream*     stream;
};

static inline void
stream_cursor_init(StreamCursor* self)
{
	self->lsn    = 0;
	self->offset = 0;
	self->page   = NULL;
	self->stream = NULL;
}

static inline void
stream_cursor_open(StreamCursor* self, Stream* stream, uint64_t lsn)
{
	// lsn to find after
	self->lsn    = lsn;
	self->stream = stream;
}

always_inline static inline StreamEvent*
stream_cursor_at(StreamCursor* self)
{
	if (unlikely(! self->page))
		return NULL;
	return (StreamEvent*)(self->page->data + self->offset);
}

hot static inline bool
stream_cursor_next(StreamCursor* self)
{
	auto stream = self->stream;
	auto lock   = &stream->lock;

	spinlock_lock(lock);

	// wait for the next available lsn (or shutdown)
	StreamWaiter waiter;
	while (self->lsn <= stream->lsn && !stream->shutdown)
	{
		stream_waiter_init(&waiter);
		list_append(&stream->waiters, &waiter.link);
		stream->waiters_count++;

		spinlock_unlock(lock);
		event_wait(&waiter.event, -1);
		spinlock_lock(lock);
	}

	// shutdown request
	if (unlikely(stream->shutdown))
	{
		spinlock_unlock(lock);
		return false;
	}

	// set first page (initial open)
	auto page = self->page;
	if (unlikely(! page))
	{
		page = container_of(list_first(&stream->pages), StreamPage, link);
		self->page   = page;
		self->offset = 0;
	}

	// rewind to the next visible event
	for (;;)
	{
		auto at = stream_cursor_at(self);
		if (at->lsn > self->lsn)
		{
			self->lsn = at->lsn;
			break;
		}
		self->offset += at->data_size;
		if (unlikely(self->offset == self->page->pos))
		{
			self->page = container_of(self->page->link.next, StreamPage, link);
			self->offset = 0;
		}
	}

	spinlock_unlock(lock);
	return true;
}
