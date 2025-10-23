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

typedef struct SetCache SetCache;

struct SetCache
{
	int  list_count;
	List list;
};

static inline void
set_cache_init(SetCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
set_cache_free(SetCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto set = list_at(Set, link);
		store_free(&set->store);
	}
	list_init(&self->list);
}

static inline Set*
set_cache_pop(SetCache* self)
{
	if (unlikely(! self->list_count))
		return NULL;
	auto first = list_pop(&self->list);
	auto set = container_of(first, Set, link);
	self->list_count--;
	return set;
}

static inline void
set_cache_push(SetCache* self, Set* set)
{
	set_reset(set);
	list_init(&set->link);
	list_append(&self->list, &set->link);
	self->list_count++;
}

static inline void
set_cache_push_list(SetCache* self, SetList* list)
{
	list_foreach_safe(&list->list)
	{
		auto set = list_at(Set, link);
		set_cache_push(self, set);
	}
	set_list_init(list);
}

static inline Set*
set_cache_create(SetCache* self, SetList* into)
{
	auto set = set_cache_pop(self);
	if (set)
		list_init(&set->link);
	else
		set = set_create();
	set_list_add(into, set);
	return set;
}
