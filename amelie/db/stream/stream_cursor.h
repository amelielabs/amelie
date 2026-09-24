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
	uint32_t     offset;
	uint64_t     id;
	uint64_t     id_open;
	StreamEvent* current;
	Page*        page;
	Stream*      stream;
};

static inline void
stream_cursor_init(StreamCursor* self)
{
	self->offset  = 0;
	self->id      = 0;
	self->id_open = 0;
	self->current = NULL;
	self->page    = NULL;
	self->stream  = NULL;
}

always_inline static inline StreamEvent*
stream_cursor_at(StreamCursor* self)
{
	return self->current;
}

static inline bool
stream_cursor_reposition(StreamCursor* self, bool with_ro)
{
	auto storage = &self->stream->storage;
	if (storage_empty(storage))
		return false;

	auto id = self->id_open;

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

		auto at = (StreamEvent*)page_at(self->page, self->offset);
		if (at->id)
		{
			// set last seen id (not related to the start position)
			self->id = at->id;
			if (at->id > id)
			{
				self->current = at;
				break;
			}
		} else
		{
			if (with_ro)
			{
				self->current = at;
				break;
			}
		}

		self->offset += sizeof(StreamEvent) + at->data_size;
	}
	return true;
}

static inline void
stream_cursor_open(StreamCursor* self, Stream* stream, uint64_t id)
{
	self->id      = 0;
	self->id_open = id;
	self->stream  = stream;

	spinlock_lock(&stream->lock);

	stream_cursor_reposition(self, false);

	spinlock_unlock(&stream->lock);
}

hot static inline bool
stream_cursor_next(StreamCursor* self)
{
	auto stream  = self->stream;
	auto storage = &stream->storage;
	auto lock    = &stream->lock;

	spinlock_lock(lock);

	if (unlikely(! self->page))
	{
		if (! stream_cursor_reposition(self, true))
		{
			spinlock_unlock(lock);
			return false;
		}
		self->current = NULL;
	}
	auto page = self->page;

	// eof
	if (self->offset == page->position && storage_is_last(storage, page->id))
	{
		spinlock_unlock(lock);
		return false;
	}

	// next event after eof
	auto at = (StreamEvent*)page_at(page, self->offset);
	if (self->current)
		self->offset += sizeof(StreamEvent) + at->data_size;

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

		at = (StreamEvent*)page_at(self->page, self->offset);
		if (at->id)
			self->id = at->id;

		self->current = at;
		break;
	}

	spinlock_unlock(lock);
	return self->current != NULL;
}
