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
	uint64_t  lsn;
	uint32_t  lsn_op;
	CdcEvent* current;
	Page*     page;
	Cdc*      cdc;
};

static inline void
cdc_cursor_init(CdcCursor* self)
{
	self->offset  = 0;
	self->lsn     = 0;
	self->lsn_op  = 0;
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
	auto storage = &cdc->storage;

	self->id  = *id;
	self->cdc = cdc;

	spinlock_lock(&cdc->lock);
	if (storage_empty(storage))
	{
		spinlock_unlock(&cdc->lock);
		return;
	}

	// rewind to match the position
	self->page   = storage_at(storage, 0);
	self->offset = sizeof(Page);
	for (;;)
	{
		// end of the page
		if (unlikely(self->offset == self->page->position))
		{
			// next page
			if (! storage_is_last(storage, self->page->id))
			{
				self->page   = storage_get(storage, self->page->id + 1);
				self->offset = sizeof(Page);
				continue;
			}
			break;
		}

		auto at = (CdcEvent*)page_at(self->page, self->offset);

		// set last seen lsn (not related to the start position)
		self->lsn    = at->lsn;
		self->lsn_op = at->lsn_op;
		if (uuid_is(&at->id, &self->id))
		{
			if (at->lsn > lsn)
			{
				self->current = at;
				break;
			}
			if (at->lsn == lsn)
			{
				if (at->lsn_op >= lsn_op)
				{
					self->current = at;
					break;
				}
			}
		}
		self->offset += sizeof(CdcEvent) + at->data_size;
	}

	spinlock_unlock(&cdc->lock);
}

hot static inline bool
cdc_cursor_next(CdcCursor* self)
{
	auto cdc     = self->cdc;
	auto page    = self->page;
	auto storage = &cdc->storage;
	auto lock    = &cdc->lock;
	if (unlikely(! page))
		return false;

	spinlock_lock(lock);

	// eof
	if (self->offset == page->position && storage_is_last(storage, page->id))
	{
		spinlock_unlock(lock);
		return false;
	}

	// next event after eof
	auto at = (CdcEvent*)page_at(page, self->offset);
	if (self->current)
		self->offset += sizeof(CdcEvent) + at->data_size;

	// rewind to the next visible event
	self->current = NULL;
	for (;;)
	{
		// end of page
		if (self->offset == self->page->position)
		{
			// next page
			if (! storage_is_last(storage, self->page->id))
			{
				self->page   = storage_get(storage, self->page->id + 1);
				self->offset = sizeof(Page);
				continue;
			}

			// no new events
			break;
		}

		at = (CdcEvent*)page_at(self->page, self->offset);
		self->lsn     = at->lsn;
		self->lsn_op  = at->lsn_op;
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
