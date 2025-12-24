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

typedef struct DtrQueue DtrQueue;

struct DtrQueue
{
	List list;
	List cache;
	int  cache_count;
};

static inline void
dtr_queue_init(DtrQueue* self)
{
	self->cache_count = 0;
	list_init(&self->list);
	list_init(&self->cache);
}

static inline void
dtr_queue_free(DtrQueue* self)
{
	list_foreach_safe(&self->cache)
	{
		auto group = list_at(DtrGroup, link);
		dtr_group_free(group);
	}
}

hot static inline void
dtr_queue_gc(DtrQueue* self, uint64_t min)
{
	list_foreach_safe(&self->list)
	{
		auto ref = list_at(DtrGroup, link);
		if (ref->id < min)
		{
			assert(! ref->list);
			list_unlink(&ref->link);
			dtr_group_reset(ref);
			list_append(&self->cache, &ref->link);
			self->cache_count++;
		}
	}
}

hot static inline DtrGroup*
dtr_queue_find(DtrQueue* self, uint64_t id)
{
	list_foreach_safe(&self->list)
	{
		auto ref = list_at(DtrGroup, link);
		if (ref->id == id)
			return ref;
	}
	return NULL;
}

hot static inline DtrGroup*
dtr_queue_add(DtrQueue* self, Dtr* dtr)
{
	// find or create new group
	auto group = dtr_queue_find(self, dtr->group);
	if (! group)
	{
		if (self->cache_count > 0)
		{
			auto first = list_pop(&self->cache);
			self->cache_count--;
			group = container_of(first, DtrGroup, link);
			list_init(&group->link);
		} else {
			group = dtr_group_allocate();
		}
		group->id = dtr->group;
		list_append(&self->list, &group->link);
	}

	dtr_group_add(group, dtr);
	return group;
}
