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
	uint32_t op;
	uint32_t offset;
	Uuid*    id;
	CdcPage* page;
	Cdc*     cdc;
};

static inline void
cdc_cursor_init(CdcCursor* self)
{
	self->op     = 0;
	self->offset = 0;
	self->id     = NULL;
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
cdc_cursor_open(CdcCursor* self, Cdc* cdc, Uuid* id, uint64_t lsn, uint32_t op)
{
	// lsn to find after
	self->op  = op;
	self->id  = id;
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
		if (uuid_is(&at->id, id))
		{
			if (at->lsn > lsn)
			{
				self->op = 0;
				break;
			}
			if (at->lsn == lsn && at->op > self->op)
			{
				self->op = at->op;
				break;
			}
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
	if (unlikely(! self->page))
		return false;

	spinlock_lock(lock);

	// eof already
	if (self->offset == page->pos && list_is_last(&cdc->pages, &page->link))
	{
		spinlock_unlock(lock);
		return false;
	}

	// set next event
	auto at = cdc_cursor_at(self);
	self->offset += sizeof(CdcEvent) + at->data_size;

	// rewind to the next visible event
	for (;;)
	{
		if (unlikely(self->offset == self->page->pos))
		{
			if (list_is_last(&cdc->pages, &self->page->link))
			{
				spinlock_unlock(lock);
				return false;
			}
			self->page   = container_of(self->page->link.next, CdcPage, link);
			self->offset = 0;
			continue;
		}

		auto at = cdc_cursor_at(self);
		if (uuid_is(&at->id, self->id))
			break;

		self->offset += sizeof(CdcEvent) + at->data_size;
	}

	spinlock_unlock(lock);
	return true;
}
