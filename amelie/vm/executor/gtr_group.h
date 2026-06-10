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

typedef struct GtrGroup GtrGroup;

struct GtrGroup
{
	uint64_t id;
	uint64_t id_next;
	Gtr*     list;
	List     link;
};

static inline GtrGroup*
gtr_group_allocate(void)
{
	auto self = (GtrGroup*)am_malloc(sizeof(GtrGroup));
	self->id      = 0;
	self->id_next = 0;
	self->list    = NULL;
	list_init(&self->link);
	return self;
}

static inline void
gtr_group_free(GtrGroup* self)
{
	am_free(self);
}

static inline void
gtr_group_reset(GtrGroup* self)
{
	self->id      = 0;
	self->id_next = 0;
	self->list    = NULL;
	list_init(&self->link);
}

static inline bool
gtr_group_pending(GtrGroup* self)
{
	return self->list && self->list->group_order == self->id_next;
}

static inline void
gtr_group_add(GtrGroup* self, Gtr* gtr)
{
	Gtr* prev = NULL;
	auto pos = self->list;
	while (pos)
	{
		if (gtr->group_order < pos->group_order)
			break;
		prev = pos;
		pos = pos->link_group;
	}
	if (prev)
	{
		gtr->link_group = prev->link_group;
		prev->link_group = gtr;
	} else
	{
		gtr->link_group = self->list;
		self->list = gtr;
	}
}

static inline Gtr*
gtr_group_pop(GtrGroup* self)
{
	assert(gtr_group_pending(self));
	auto next = self->list;
	self->list = next->link_group;
	self->id_next++;
	next->link_group = NULL;
	return next;
}
