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

typedef struct PtrCache PtrCache;

struct PtrCache
{
	List list;
};

static inline void
ptr_cache_init(PtrCache* self)
{
	list_init(&self->list);
}

static inline void
ptr_cache_free(PtrCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto ptr = list_at(Ptr, link);
		ptr_free(ptr);
	}
	list_init(&self->list);
}

static inline Ptr*
ptr_cache_pop(PtrCache* self)
{
	if (list_empty(&self->list))
		return NULL;
	auto first = list_pop(&self->list);
	return container_of(first, Ptr, link);
}

static inline void
ptr_cache_push(PtrCache* self, Ptr* ptr)
{
	ptr_reset(ptr);
	list_append(&self->list, &ptr->link);
}

hot static inline Ptr*
ptr_create(PtrCache* self, Part* part, Dtr* dtr, Complete* complete)
{
	auto ptr = ptr_cache_pop(self);
	if (! ptr)
		ptr = ptr_allocate(dtr, complete);
	else
		list_init(&ptr->link);
	ptr->part = part;
	return ptr;
}
