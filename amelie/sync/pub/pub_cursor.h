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

typedef struct PubCursor PubCursor;

struct PubCursor
{
	uint64_t lsn;
	uint32_t offset;
	PubPage* page;
	Pub*     pub;
};

static inline void
pub_cursor_init(PubCursor* self)
{
	self->lsn    = 0;
	self->offset = 0;
	self->page   = NULL;
	self->pub    = NULL;
}

static inline void
pub_cursor_open(PubCursor* self, Pub* pub, uint64_t lsn)
{
	// lsn to find after
	self->lsn = lsn;
	self->pub = pub;
}

always_inline static inline PubEvent*
pub_cursor_at(PubCursor* self)
{
	if (unlikely(! self->page))
		return NULL;
	return (PubEvent*)(self->page->data + self->offset);
}

hot static inline bool
pub_cursor_next(PubCursor* self)
{
	auto pub  = self->pub;
	auto lock = &pub->lock;

	spinlock_lock(lock);

	// wait for the next available lsn (or shutdown)
	PubWaiter waiter;
	while (self->lsn <= pub->lsn && !pub->shutdown)
	{
		pub_waiter_init(&waiter);
		list_append(&pub->waiters, &waiter.link);
		pub->waiters_count++;

		spinlock_unlock(lock);
		event_wait(&waiter.event, -1);
		spinlock_lock(lock);
	}

	// shutdown request
	if (unlikely(pub->shutdown))
	{
		spinlock_unlock(lock);
		return false;
	}

	// set first page (initial open)
	auto page = self->page;
	if (unlikely(! page))
	{
		page = container_of(list_first(&pub->pages), PubPage, link);
		self->page   = page;
		self->offset = 0;
	}

	// rewind to the next visible event
	for (;;)
	{
		auto at = pub_cursor_at(self);
		if (at->lsn > self->lsn)
		{
			self->lsn = at->lsn;
			break;
		}
		self->offset += at->data_size;
		if (unlikely(self->offset == self->page->pos))
		{
			self->page = container_of(self->page->link.next, PubPage, link);
			self->offset = 0;
		}
	}

	spinlock_unlock(lock);
	return true;
}
