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

typedef struct TrCache TrCache;

struct TrCache
{
	int  list_count;
	List list;
};

static inline void
tr_cache_init(TrCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
tr_cache_free(TrCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto tr = list_at(Tr, link);
		tr_free(tr);
	}
}

static inline Tr*
tr_cache_pop(TrCache* self)
{
	Tr* tr = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		tr = container_of(first, Tr, link);
		self->list_count--;
	}
	return tr;
}

static inline void
tr_cache_push(TrCache* self, Tr* tr)
{
	tr_reset(tr);
	list_append(&self->list, &tr->link);
	self->list_count++;
}

static inline Tr*
tr_create(TrCache* self)
{
	auto tr = tr_cache_pop(self);
	if (! tr)
		tr = tr_allocate();
	return tr;
}
