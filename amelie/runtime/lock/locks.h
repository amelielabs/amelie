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

typedef struct Locks Locks;

struct Locks
{
	Spinlock  lock;
	List      list;
	int       list_count;
	LockCache cache;
};

static inline void
locks_init(Locks* self)
{
	self->list_count = 0;
	list_init(&self->list);
	spinlock_init(&self->lock);
	lock_cache_init(&self->cache);
}

static inline void
locks_free(Locks* self)
{
	spinlock_free(&self->lock);
	lock_cache_free(&self->cache);
}

hot static inline void
locks_add(Locks* self, Lock* lock)
{
	spinlock_lock(&self->lock);
	list_append(&self->list, &lock->link_mgr);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

hot static inline void
locks_remove(Locks* self, Lock* lock)
{
	spinlock_lock(&self->lock);
	list_unlink(&lock->link_mgr);
	self->list_count--;
	assert(self->list_count >= 0);
	spinlock_unlock(&self->lock);
}

hot static inline bool
lock_resolve(Resource* res, LockId lock)
{
	// validate lock against currently active locks
	auto set = res->lock_set;
	if (set[LOCK_EXCLUSIVE])
		return false;

	if (lock == LOCK_EXCLUSIVE)
		return !set[LOCK_SHARED];

	// shared
	return true;
}

hot static inline void
locks_lock_of(Locks* self, Lock* lock)
{
	auto res = lock->res;

	// try to lock the resource
	spinlock_lock(&res->lock);

	// add lock to the lock manager
	locks_add(self, lock);
	auto event = &lock->event;

	// always wait if there are waiters
	auto pass = !res->lock_wait_count && lock_resolve(res, lock->lock);
	if (likely(pass))
	{
		// success
		res->lock_set[lock->lock]++;
	} else
	{
		// add lock to the wait list
		lock->waiting = true;
		list_append(&res->lock_wait, &lock->link);
		res->lock_wait_count++;

		// set bus
		event_attach(event);
	}

	spinlock_unlock(&res->lock);

	// wait for the lock
	if (unlikely(! pass))
		event_wait(event, -1);
}

hot static inline Lock*
locks_lock(Locks*      self, Resource* res, LockId id,
           Str*        name,
           const char* func)
{
	if (unlikely(id == LOCK_NONE))
		return NULL;

	// reentrant support
	list_foreach(&am_self()->locks)
	{
		auto lock = list_at(Lock, link);
		if (lock->res == res && lock->lock == id)
		{
			lock->refs++;
			return lock;
		}
	}

	// create lock
	auto lock = lock_create(&self->cache, res, id, name, func);
	locks_lock_of(self, lock);

	// attach lock to the coroutine
	lock_attach(lock);
	lock->refs++;
	return lock;
}

hot static inline void
locks_unlock(Locks* self, Lock* lock)
{
	if (!lock || lock->lock == LOCK_NONE)
		return;

	// reentrant support
	lock->refs--;
	if (lock->refs > 0)
		return;

	// detach from the coroutine
	if (lock->coro)
		lock_detach(lock);

	auto res = lock->res;
	spinlock_lock(&res->lock);

	// detach from the lock set
	res->lock_set[lock->lock]--;

	// detach from the lock manager
	locks_remove(self, lock);

	// batch wakeup non-conflicting waiters
	while (res->lock_wait_count > 0)
	{
		auto lock = container_of(list_first(&res->lock_wait), Lock, link);
		if (lock_resolve(lock->res, lock->lock))
		{
			res->lock_set[lock->lock]++;
			res->lock_wait_count--;
			lock->waiting = false;
			list_unlink(&lock->link);
			event_signal(&lock->event);
			continue;
		}
		break;
	}

	spinlock_unlock(&lock->res->lock);

	lock->res  = NULL;
	lock->lock = LOCK_NONE;

	lock_cache_push(&self->cache, lock);
}

hot static inline Lock*
locks_find(Locks* self, Str* name)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	list_foreach(&self->list)
	{
		auto lock = list_at(Lock, link_mgr);
		if (str_empty(&lock->name))
			continue;
		if (str_compare(&lock->name, name))
			return lock;
	}
	return NULL;
}

static inline void
locks_list(Locks* self, Buf* buf)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	encode_array(buf);
	list_foreach(&self->list)
	{
		auto lock = list_at(Lock, link_mgr);
		lock_write(lock, buf);
	}
	encode_array_end(buf);
}

static inline int
locks_count(Locks* self, Str* name)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	auto count = 0;
	list_foreach(&self->list)
	{
		auto lock = list_at(Lock, link_mgr);
		if (str_is_cstr(name, lock->res->name))
			count++;
	}
	return count;
}
