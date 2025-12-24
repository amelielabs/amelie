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

typedef struct DtrGroup DtrGroup;

struct DtrGroup
{
	uint64_t id;
	uint64_t id_next;
	Dtr*     list;
	List     link;
};

static inline DtrGroup*
dtr_group_allocate(void)
{
	auto self = (DtrGroup*)am_malloc(sizeof(DtrGroup));
	self->id      = 0;
	self->id_next = 0;
	self->list    = NULL;
	list_init(&self->link);
	return self;
}

static inline void
dtr_group_free(DtrGroup* self)
{
	am_free(self);
}

static inline void
dtr_group_reset(DtrGroup* self)
{
	self->id      = 0;
	self->id_next = 0;
	self->list    = NULL;
	list_init(&self->link);
}

static inline bool
dtr_group_pending(DtrGroup* self)
{
	return self->list && self->list->group_order == self->id_next;
}

static inline void
dtr_group_add(DtrGroup* self, Dtr* dtr)
{
	Dtr* prev = NULL;
	auto pos = self->list;
	while (pos)
	{
		if (dtr->group_order < pos->group_order)
			break;
		prev = pos;
		pos = pos->link_group;
	}
	if (prev)
	{
		dtr->link_group = prev->link_group;
		prev->link_group = dtr;
	} else
	{
		dtr->link_group = self->list;
		self->list = dtr;
	}
}

static inline Dtr*
dtr_group_pop(DtrGroup* self)
{
	assert(dtr_group_pending(self));
	auto next = self->list;
	self->list = next->link_group;
	self->id_next++;
	next->link_group = NULL;
	return next;
}
