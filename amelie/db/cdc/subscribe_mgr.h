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

typedef struct SubscribeMgr SubscribeMgr;

struct SubscribeMgr
{
	List list;
	int  list_count;
	Cdc* cdc;
};

static inline void
subscribe_mgr_init(SubscribeMgr* self, Cdc* cdc)
{
	self->cdc        = cdc;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
subscribe_mgr_free(SubscribeMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto sub = list_at(Subscribe, link);
		cdc_detach(self->cdc, &sub->slot);
		subscribe_free(sub);
	}
	list_init(&self->list);
	self->list_count = 0;
}

static inline bool
subscribe_mgr_empty(SubscribeMgr* self)
{
	return !self->list_count;
}

static inline void
subscribe_mgr_add(SubscribeMgr* self, Subscribe* sub)
{
	list_append(&self->list, &sub->link);
	self->list_count++;
	cdc_attach(self->cdc, &sub->slot);
}

static inline void
subscribe_mgr_remove(SubscribeMgr* self, Subscribe* sub)
{
	list_unlink(&sub->link);
	self->list_count--;
	cdc_detach(self->cdc, &sub->slot);
}

static inline Subscribe*
subscribe_mgr_find(SubscribeMgr* self, Str* user, Str* name)
{
	list_foreach(&self->list)
	{
		auto sub = list_at(Subscribe, link);
		if (str_compare(&sub->user, user) &&
		    str_compare(&sub->name, name))
			return sub;
	}
	return NULL;
}

static inline uint64_t
subscribe_mgr_min(SubscribeMgr* self)
{
	uint64_t min = UINT64_MAX;
	list_foreach(&self->list)
	{
		auto sub = list_at(Subscribe, link);
		auto lsn = atomic_u64_of(&sub->slot.lsn);
		if (lsn < min)
			min = lsn;
	}
	return min;
}

hot static inline void
subscribe_mgr_collect(SubscribeMgr* self, Buf* buf)
{
	list_foreach(&self->list)
	{
		auto sub = list_at(Subscribe, link);
		for (;;)
		{
			if (! cdc_cursor_next(&sub->cursor))
				break;
			auto event = cdc_cursor_at(&sub->cursor);
			cdc_export(buf, &sub->user, &sub->name, event);
		}
		cdc_slot_set(&sub->slot, sub->cursor.lsn, 0);
	}

	// todo: move subs to tail
}
