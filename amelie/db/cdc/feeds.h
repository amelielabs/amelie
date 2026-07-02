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

typedef struct Feeds Feeds;

struct Feeds
{
	List list;
	int  list_count;
	Cdc* cdc;
};

static inline void
feeds_init(Feeds* self, Cdc* cdc)
{
	self->cdc        = cdc;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
feeds_free(Feeds* self)
{
	list_foreach_safe(&self->list)
	{
		auto sub = list_at(Feed, link);
		cdc_detach(self->cdc, &sub->slot);
		feed_free(sub);
	}
	list_init(&self->list);
	self->list_count = 0;
}

static inline bool
feeds_empty(Feeds* self)
{
	return !self->list_count;
}

static inline void
feeds_add(Feeds* self, Feed* sub)
{
	list_append(&self->list, &sub->link);
	self->list_count++;
	cdc_attach(self->cdc, &sub->slot);
}

static inline void
feeds_remove(Feeds* self, Feed* sub)
{
	list_unlink(&sub->link);
	self->list_count--;
	cdc_detach(self->cdc, &sub->slot);
}

static inline Feed*
feeds_find(Feeds* self, Str* user, Str* name)
{
	list_foreach(&self->list)
	{
		auto sub = list_at(Feed, link);
		if (str_compare(&sub->user, user) &&
		    str_compare(&sub->name, name))
			return sub;
	}
	return NULL;
}

static inline uint64_t
feeds_min(Feeds* self)
{
	uint64_t min = UINT64_MAX;
	list_foreach(&self->list)
	{
		auto sub = list_at(Feed, link);
		auto lsn = atomic_u64_of(&sub->slot.lsn);
		if (lsn < min)
			min = lsn;
	}
	return min;
}

hot static inline void
feeds_collect(Feeds* self, Buf* buf)
{
	list_foreach(&self->list)
	{
		auto feed = list_at(Feed, link);
		for (;;)
		{
			auto event = cdc_cursor_at(&feed->cursor);
			if (event)
				cdc_export(buf, &feed->user, &feed->name, event);
			if (! cdc_cursor_next(&feed->cursor))
				break;
		}
		cdc_slot_set(&feed->slot, feed->cursor.lsn, feed->cursor.lsn_op);
	}

	// todo: move to tail
}
