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

typedef struct CdcCursor CdcCursor;

struct CdcCursor
{
	uint64_t lsn;
	uint32_t offset;
	CdcPage* page;
	Cdc*     cdc;
};

static inline void
cdc_cursor_init(CdcCursor* self)
{
	self->lsn    = 0;
	self->offset = 0;
	self->page   = NULL;
	self->cdc    = NULL;
}

static inline void
cdc_cursor_open(CdcCursor* self, Cdc* cdc, uint64_t lsn)
{
	// lsn to find after
	self->lsn = lsn;
	self->cdc = cdc;
}

always_inline static inline CdcEvent*
cdc_cursor_at(CdcCursor* self)
{
	if (unlikely(! self->page))
		return NULL;
	return (CdcEvent*)(self->page->data + self->offset);
}

hot static inline bool
cdc_cursor_next(CdcCursor* self)
{
	auto cdc  = self->cdc;
	auto lock = &cdc->lock;

	spinlock_lock(lock);

	if (cdc->lsn <= self->lsn || cdc->shutdown)
	{
		spinlock_unlock(lock);
		return false;
	}

	// set first page (initial open)
	auto page = self->page;
	if (unlikely(! page))
	{
		page = container_of(list_first(&cdc->pages), CdcPage, link);
		self->page   = page;
		self->offset = 0;
	}

	// rewind to the next visible event
	for (;;)
	{
		auto at = cdc_cursor_at(self);
		if (at->lsn > self->lsn)
		{
			self->lsn = at->lsn;
			break;
		}
		self->offset += sizeof(CdcEvent) + at->data_size;
		if (unlikely(self->offset == self->page->pos))
		{
			self->page = container_of(self->page->link.next, CdcPage, link);
			self->offset = 0;
		}
	}

	spinlock_unlock(lock);
	return true;
}
