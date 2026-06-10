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

typedef struct GtrQueue GtrQueue;

struct GtrQueue
{
	List list;
	List cache;
	int  cache_count;
};

static inline void
gtr_queue_init(GtrQueue* self)
{
	self->cache_count = 0;
	list_init(&self->list);
	list_init(&self->cache);
}

static inline void
gtr_queue_free(GtrQueue* self)
{
	list_foreach_safe(&self->cache)
	{
		auto group = list_at(GtrGroup, link);
		gtr_group_free(group);
	}
}

hot static inline void
gtr_queue_gc(GtrQueue* self, uint64_t min)
{
	list_foreach_safe(&self->list)
	{
		auto ref = list_at(GtrGroup, link);
		if (ref->id < min)
		{
			assert(! ref->list);
			list_unlink(&ref->link);
			gtr_group_reset(ref);
			list_append(&self->cache, &ref->link);
			self->cache_count++;
		}
	}
}

hot static inline GtrGroup*
gtr_queue_find(GtrQueue* self, uint64_t id)
{
	list_foreach_safe(&self->list)
	{
		auto ref = list_at(GtrGroup, link);
		if (ref->id == id)
			return ref;
	}
	return NULL;
}

hot static inline GtrGroup*
gtr_queue_add(GtrQueue* self, Gtr* gtr)
{
	// find or create new group
	auto group = gtr_queue_find(self, gtr->group);
	if (! group)
	{
		if (self->cache_count > 0)
		{
			auto first = list_pop(&self->cache);
			self->cache_count--;
			group = container_of(first, GtrGroup, link);
			list_init(&group->link);
		} else {
			group = gtr_group_allocate();
		}
		group->id = gtr->group;
		list_append(&self->list, &group->link);
	}

	gtr_group_add(group, gtr);
	return group;
}
