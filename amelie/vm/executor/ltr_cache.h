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

typedef struct LtrCache LtrCache;

struct LtrCache
{
	List list;
};

static inline void
ltr_cache_init(LtrCache* self)
{
	list_init(&self->list);
}

static inline void
ltr_cache_free(LtrCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto ltr = list_at(Ltr, link);
		ltr_free(ltr);
	}
	list_init(&self->list);
}

static inline Ltr*
ltr_cache_pop(LtrCache* self)
{
	if (list_empty(&self->list))
		return NULL;
	auto first = list_pop(&self->list);
	return container_of(first, Ltr, link);
}

static inline void
ltr_cache_push(LtrCache* self, Ltr* ltr)
{
	ltr_reset(ltr);
	list_append(&self->list, &ltr->link);
}

hot static inline Ltr*
ltr_create(LtrCache* self, Part* part, Dtr* dtr, Complete* complete)
{
	auto ltr = ltr_cache_pop(self);
	if (! ltr)
		ltr = ltr_allocate(dtr, complete);
	else
		list_init(&ltr->link);
	ltr->part = part;
	return ltr;
}
