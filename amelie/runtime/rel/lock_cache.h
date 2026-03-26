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

typedef struct LockCache LockCache;

struct LockCache
{
	Spinlock lock;
	List     list;
};

static inline void
lock_cache_init(LockCache* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
lock_cache_free(LockCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto lock = list_at(Lock, link);
		lock_free(lock);
	}
	list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline Lock*
lock_cache_pop(LockCache* self)
{
	spinlock_lock(&self->lock);
	Lock* lock = NULL;
	if (! list_empty(&self->list))
	{
		auto first = list_pop(&self->list);
		lock = container_of(first, Lock, link);
	}
	spinlock_unlock(&self->lock);
	return lock;
}

static inline void
lock_cache_push(LockCache* self, Lock* lock)
{
	lock_reset(lock);
	spinlock_lock(&self->lock);
	list_append(&self->list, &lock->link);
	spinlock_unlock(&self->lock);
}

static inline Lock*
lock_create(LockCache* self, Rel* rel, LockId rel_lock,
            Str*       name, const char* func)
{
	auto lock = lock_cache_pop(self);
	if (unlikely(! lock))
		lock = malloc(sizeof(Lock));
	lock_init(lock, rel, rel_lock, name, func);
	return lock;
}
