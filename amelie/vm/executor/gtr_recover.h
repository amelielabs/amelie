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

typedef struct GtrRecover GtrRecover;

struct GtrRecover
{
	uint64_t id;
	uint64_t id_next;
	Gtr*     list;
};

static inline void
gtr_recover_init(GtrRecover* self)
{
	self->id      = 0;
	self->id_next = 0;
	self->list    = NULL;
}

static inline void
gtr_recover_reset(GtrRecover* self)
{
	self->id      = 0;
	self->id_next = 0;
	self->list    = NULL;
}

static inline bool
gtr_recover_pending(GtrRecover* self)
{
	return self->list && self->list->write.recover_id == self->id_next;
}

static inline void
gtr_recover_add(GtrRecover* self, Gtr* gtr)
{
	Gtr* prev = NULL;
	auto pos = self->list;
	while (pos)
	{
		if (gtr->write.recover_id < pos->write.recover_id)
			break;
		prev = pos;
		pos = pos->link_recover;
	}
	if (prev)
	{
		gtr->link_recover = prev->link_recover;
		prev->link_recover = gtr;
	} else
	{
		gtr->link_recover = self->list;
		self->list = gtr;
	}
}

static inline Gtr*
gtr_recover_pop(GtrRecover* self)
{
	assert(gtr_recover_pending(self));
	auto next = self->list;
	self->list = next->link_recover;
	self->id_next++;
	next->link_recover = NULL;
	return next;
}
