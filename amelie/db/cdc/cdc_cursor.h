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
	Uuid      id;
	uint32_t  offset;
	uint64_t  pos;
	uint32_t  pos_op;
	CdcEvent* current;
	CdcPage*  page;
	Cdc*      cdc;
};

static inline void
cdc_cursor_init(CdcCursor* self)
{
	self->offset  = 0;
	self->pos     = 0;
	self->pos_op  = 0;
	self->current = NULL;
	self->page    = NULL;
	self->cdc     = NULL;
	uuid_init(&self->id);
}

always_inline static inline CdcEvent*
cdc_cursor_at(CdcCursor* self)
{
	return self->current;
}

static inline void
cdc_cursor_open(CdcCursor* self, Cdc* cdc, Uuid* id,
                uint64_t   lsn,
                uint32_t   lsn_op)
{
	self->id  = *id;
	self->cdc = cdc;

	spinlock_lock(&cdc->lock);

	// rewind to match the position
	self->page   = container_of(list_first(&cdc->pages), CdcPage, link);
	self->offset = 0;
	for (;;)
	{
		// end of page
		if (unlikely(self->offset == self->page->pos))
		{
			// next page
			if (! list_is_last(&cdc->pages, &self->page->link))
			{
				self->page   = container_of(self->page->link.next, CdcPage, link);
				self->offset = 0;
				continue;
			}
			break;
		}
		auto at = (CdcEvent*)(self->page->data + self->offset);

		// set last seen lsn (not related to the start position)
		self->pos    = at->lsn;
		self->pos_op = at->op;

		if (at->lsn >= lsn)
		{
			if (at->op >= lsn_op)
			{
				self->current = at;
				break;
			}
		}
		self->offset += sizeof(CdcEvent) + at->data_size;
	}

	spinlock_unlock(&cdc->lock);
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

	// eof
	if (self->offset == page->pos && list_is_last(&cdc->pages, &page->link))
	{
		spinlock_unlock(lock);
		return false;
	}

	// next event after eof
	auto at = (CdcEvent*)(self->page->data + self->offset);
	if (self->current)
		self->offset += sizeof(CdcEvent) + at->data_size;

	// rewind to the next visible event
	self->current = NULL;
	for (;;)
	{
		// end of page
		if (self->offset == self->page->pos)
		{
			// next page
			if (! list_is_last(&cdc->pages, &self->page->link))
			{
				self->page   = container_of(self->page->link.next, CdcPage, link);
				self->offset = 0;
				continue;
			}

			// no new events
			break;
		}

		at = (CdcEvent*)(self->page->data + self->offset);
		self->pos     = at->lsn;
		self->pos_op  = at->op;
		if (uuid_is(&at->id, &self->id))
		{
			self->current = at;
			break;
		}

		// iterate forward
		self->offset += sizeof(CdcEvent) + at->data_size;
	}

	spinlock_unlock(lock);
	return self->current != NULL;
}
