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

typedef struct FlatMgr FlatMgr;

struct FlatMgr
{
	List list;
	int  list_count;
};

static inline void
flat_mgr_init(FlatMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
flat_mgr_free(FlatMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto flat = list_at(Flat, link);
		flat_free(flat);
	}
	list_init(&self->list);
	self->list_count = 0;
}

static inline void
flat_mgr_add(FlatMgr* self, Flat* flat)
{
	list_append(&self->list, &flat->link);
	self->list_count++;
}
