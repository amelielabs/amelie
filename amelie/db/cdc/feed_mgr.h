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

typedef struct FeedMgr FeedMgr;

struct FeedMgr
{
	List list;
	int  list_count;
	Cdc* cdc;
};

static inline void
feed_mgr_init(FeedMgr* self, Cdc* cdc)
{
	self->cdc        = cdc;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
feed_mgr_free(FeedMgr* self)
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
feed_mgr_empty(FeedMgr* self)
{
	return !self->list_count;
}

static inline void
feed_mgr_add(FeedMgr* self, Feed* sub)
{
	list_append(&self->list, &sub->link);
	self->list_count++;
	cdc_attach(self->cdc, &sub->slot);
}

static inline void
feed_mgr_remove(FeedMgr* self, Feed* sub)
{
	list_unlink(&sub->link);
	self->list_count--;
	cdc_detach(self->cdc, &sub->slot);
}

static inline Feed*
feed_mgr_find(FeedMgr* self, Str* user, Str* name)
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
feed_mgr_min(FeedMgr* self)
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
feed_mgr_collect(FeedMgr* self, Buf* buf)
{
	list_foreach(&self->list)
	{
		auto sub = list_at(Feed, link);
		for (;;)
		{
			if (! cdc_cursor_next(&sub->cursor))
				break;
			auto event = cdc_cursor_at(&sub->cursor);
			cdc_export(buf, &sub->user, &sub->name, event);
		}
		cdc_slot_set(&sub->slot, sub->cursor.lsn, 0);
	}

	// todo: move to tail
}
