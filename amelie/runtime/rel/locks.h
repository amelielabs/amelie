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
lock_resolve(Rel* rel, LockId lock)
{
	// validate lock against currently active locks
	auto set = rel->lock_set;
	if (set[LOCK_EXCLUSIVE])
		return false;
	switch (lock) {
	case LOCK_SHARED:
		return true;
	case LOCK_SHARED_RW:
		return !set[LOCK_EXCLUSIVE_RO];
	case LOCK_EXCLUSIVE_RO:
		return !set[LOCK_SHARED_RW];
	case LOCK_EXCLUSIVE:
		return !set[LOCK_SHARED]       &&
		       !set[LOCK_SHARED_RW]    &&
		       !set[LOCK_EXCLUSIVE_RO] &&
		       !set[LOCK_CALL];
	case LOCK_CALL:
		return true;
	default:
		abort();
	}
	// conflict
	return false;
}

hot static inline void
locks_lock_of(Locks* self, Lock* lock)
{
	auto rel = lock->rel;

	// try to lock the relation
	spinlock_lock(&rel->lock);

	// add lock to the lock manager
	locks_add(self, lock);

	// always wait if there are waiters
	auto pass = !rel->lock_wait_count && lock_resolve(rel, lock->rel_lock);
	if (likely(pass))
	{
		// success
		rel->lock_set[lock->rel_lock]++;
	} else
	{
		// add lock to the relation wait list
		lock->waiting = true;
		list_append(&rel->lock_wait, &lock->link);
		rel->lock_wait_count++;
	}

	spinlock_unlock(&rel->lock);

	// wait for the lock
	if (unlikely(! pass))
	{
		auto event = &lock->event;
		event_attach(event);
		event_wait(event, -1);
	}
}

hot static inline Lock*
locks_lock(Locks*      self, Rel* rel, LockId rel_lock,
           Str*        name,
           const char* func)
{
	if (unlikely(rel_lock == LOCK_NONE))
		return NULL;

	// reentrant support
	list_foreach(&am_self()->locks)
	{
		auto lock = list_at(Lock, link);
		if (lock->rel == rel && lock->rel_lock == rel_lock)
		{
			lock->refs++;
			return lock;
		}
	}

	// create lock
	auto lock = lock_create(&self->cache, rel, rel_lock, name, func);
	locks_lock_of(self, lock);

	// attach lock to the coroutine
	lock_attach(lock);
	lock->refs++;
	return lock;
}

hot static inline void
locks_lock_access(Locks*      self, Access* access,
                  const char* func)
{
	// do nothing if the access list is empty
	if (access_empty(access))
		return;

	// lock all relations from the access list
	//
	// all access relations are ordered by relation->lock_order,
	// which should prevent deadlocks
	//
	for (auto at = 0; at < access->list_count; at++)
	{
		auto record = access_at_ordered(access, at);
		locks_lock(self, record->rel, record->lock, NULL, func);
	}
}

hot static inline void
locks_unlock(Locks* self, Lock* lock)
{
	if (!lock || lock->rel_lock == LOCK_NONE)
		return;

	// reentrant support
	lock->refs--;
	if (lock->refs > 0)
		return;

	// detach from the coroutine
	if (lock->coro)
		lock_detach(lock);

	auto rel = lock->rel;
	spinlock_lock(&rel->lock);

	// detach from the relation lock set
	rel->lock_set[lock->rel_lock]--;

	// detach from the lock manager
	locks_remove(self, lock);

	// batch wakeup non-conflicting waiters
	while (rel->lock_wait_count > 0)
	{
		auto lock = container_of(list_first(&rel->lock_wait), Lock, link);
		if (lock_resolve(lock->rel, lock->rel_lock))
		{
			rel->lock_set[lock->rel_lock]++;
			rel->lock_wait_count--;
			lock->waiting = false;
			list_unlink(&lock->link);
			event_signal(&lock->event);
			continue;
		}
		break;
	}

	spinlock_unlock(&lock->rel->lock);

	lock->rel      = NULL;
	lock->rel_lock = LOCK_NONE;

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
locks_count(Locks* self, Str* rel_name)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	auto count = 0;
	list_foreach(&self->list)
	{
		auto lock = list_at(Lock, link_mgr);
		if (str_compare(lock->rel->name, rel_name))
			count++;
	}
	return count;
}
