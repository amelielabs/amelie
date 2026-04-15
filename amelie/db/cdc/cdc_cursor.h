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
	uint32_t op;
	uint32_t offset;
	CdcPage* page;
	Cdc*     cdc;
};

static inline void
cdc_cursor_init(CdcCursor* self)
{
	self->lsn    = 0;
	self->op     = 0;
	self->offset = 0;
	self->page   = NULL;
	self->cdc    = NULL;
}

always_inline static inline CdcEvent*
cdc_cursor_at(CdcCursor* self)
{
	if (unlikely(! self->page))
		return NULL;
	return (CdcEvent*)(self->page->data + self->offset);
}

static inline void
cdc_cursor_open(CdcCursor* self, Cdc* cdc, uint64_t lsn, uint32_t op)
{
	// lsn to find after
	self->lsn = lsn;
	self->op  = op;
	self->cdc = cdc;

	auto lock = &cdc->lock;
	spinlock_lock(lock);

	auto page = container_of(list_first(&cdc->pages), CdcPage, link);
	self->page   = page;
	self->offset = 0;

	// rewind to the next visible event
	for (;;)
	{
		if (unlikely(self->offset == self->page->pos))
		{
			if (list_is_last(&cdc->pages, &page->link))
			{
				self->page = NULL;
				self->offset = 0;
				break;
			}

			self->page = container_of(self->page->link.next, CdcPage, link);
			self->offset = 0;
			continue;
		}
		auto at = cdc_cursor_at(self);
		if (at->lsn > self->lsn)
		{
			self->lsn = at->lsn;
			self->op  = 0;
			break;
		}
		if (at->lsn == self->lsn && at->op > self->op)
		{
			self->op = at->op;
			break;
		}
		self->offset += sizeof(CdcEvent) + at->data_size;
	}

	spinlock_unlock(lock);
}

hot static inline bool
cdc_cursor_next(CdcCursor* self)
{
	auto cdc  = self->cdc;
	auto lock = &cdc->lock;

	auto page = self->page;
	if (! page)
		return false;

	spinlock_lock(lock);
	if (self->offset == page->pos && list_is_last(&cdc->pages, &page->link))
	{
		spinlock_unlock(lock);
		return false;
	}

	// set next event
	auto at = cdc_cursor_at(self);
	self->offset += sizeof(CdcEvent) + at->data_size;

	if (unlikely(self->offset == self->page->pos))
	{
		if (list_is_last(&cdc->pages, &self->page->link))
			self->page = NULL;
		else
			self->page = container_of(self->page->link.next, CdcPage, link);
		self->offset = 0;
	}

	spinlock_unlock(lock);
	return self->page != NULL;
}
