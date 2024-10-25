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

typedef struct Lock      Lock;
typedef struct LockCache LockCache;
typedef struct Resource  Resource;

struct Lock
{
	int        refs;
	bool       shared;
	Resource*  resource;
	Coroutine* coroutine;
	List       link;
};

struct LockCache
{
	List list;
	int  list_count;
};

static inline void
lock_cache_init(LockCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
lock_cache_free(LockCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto lock = list_at(Lock, link);
		am_free(lock);
	}
}

static inline Lock*
lock_cache_pop(LockCache* self)
{
	Lock* lock = NULL;
	if (self->list_count > 0)
	{
		auto first = list_pop(&self->list);
		lock = container_of(first, Lock, link);
		self->list_count--;
	} else
	{
		lock = am_malloc(sizeof(Lock));
		list_init(&lock->link);
	}
	lock->refs      = 0;
	lock->shared    = false;
	lock->coroutine = am_self();
	return lock;
}

static inline void
lock_cache_push(LockCache* self, Lock* lock)
{
	lock->refs      = 0;
	lock->shared    = false;
	lock->resource  = NULL;
	lock->coroutine = NULL;
	self->list_count++;
	list_append(&self->list, &lock->link);
}

static inline Lock*
lock_create(LockCache* self, Resource* resource, bool shared)
{
	auto lock = lock_cache_pop(self);
	lock->shared   = shared;
	lock->resource = resource;
	return lock;
}
