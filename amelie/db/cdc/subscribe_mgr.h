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
