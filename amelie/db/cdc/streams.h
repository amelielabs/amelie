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

typedef struct Streams Streams;

struct Streams
{
	List list;
	int  list_count;
	Cdc* cdc;
};

static inline void
streams_init(Streams* self, Cdc* cdc)
{
	self->cdc        = cdc;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
streams_free(Streams* self)
{
	list_foreach_safe(&self->list)
	{
		auto sub = list_at(Stream, link);
		cdc_detach(self->cdc, &sub->slot);
		stream_free(sub);
	}
	list_init(&self->list);
	self->list_count = 0;
}

static inline bool
streams_empty(Streams* self)
{
	return !self->list_count;
}

static inline void
streams_add(Streams* self, Stream* sub)
{
	list_append(&self->list, &sub->link);
	self->list_count++;
	cdc_attach(self->cdc, &sub->slot);
}

static inline void
streams_remove(Streams* self, Stream* sub)
{
	list_unlink(&sub->link);
	self->list_count--;
	cdc_detach(self->cdc, &sub->slot);
}

static inline Stream*
streams_find(Streams* self, Str* user, Str* name)
{
	list_foreach(&self->list)
	{
		auto sub = list_at(Stream, link);
		if (str_compare(&sub->user, user) &&
		    str_compare(&sub->name, name))
			return sub;
	}
	return NULL;
}

static inline uint64_t
streams_min(Streams* self)
{
	uint64_t min = UINT64_MAX;
	list_foreach(&self->list)
	{
		auto sub = list_at(Stream, link);
		auto lsn = atomic_u64_of(&sub->slot.lsn);
		if (lsn < min)
			min = lsn;
	}
	return min;
}

hot static inline void
streams_collect(Streams* self, Buf* buf)
{
	list_foreach(&self->list)
	{
		auto feed = list_at(Stream, link);
		for (;;)
		{
			auto event = cdc_cursor_at(&feed->cursor);
			if (event)
			{
				buf_write(buf, "data: ", 6);
				cdc_export(buf, &feed->user, &feed->name, event);
				buf_write(buf, "\n\n", 2);
			}
			if (! cdc_cursor_next(&feed->cursor))
				break;
		}
		cdc_slot_set(&feed->slot, feed->cursor.lsn);
	}

	// todo: move to tail
}
