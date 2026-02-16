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

typedef struct LockMgr LockMgr;

struct LockMgr
{
	Spinlock  lock;
	List      list;
	int       list_count;
};

static inline void
lock_mgr_init(LockMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
	spinlock_init(&self->lock);
}

static inline void
lock_mgr_free(LockMgr* self)
{
	spinlock_free(&self->lock);
}

hot static inline void
lock_mgr_add(LockMgr* self, Lock* lock)
{
	spinlock_lock(&self->lock);
	list_append(&self->list, &lock->link_mgr);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

hot static inline void
lock_mgr_remove(LockMgr* self, Lock* lock)
{
	spinlock_lock(&self->lock);
	list_unlink(&lock->link_mgr);
	self->list_count--;
	assert(self->list_count >= 0);
	spinlock_unlock(&self->lock);
}

hot static inline bool
lock_resolve(Relation* rel, LockId lock)
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
lock_mgr_lock_of(LockMgr* self, Lock* lock)
{
	auto rel = lock->rel;

	// try to lock the relation
	spinlock_lock(&rel->lock);

	// add lock to the lock manager
	lock_mgr_add(self, lock);

	// always wait if there are waiters
	auto pass = !rel->lock_wait_count && lock_resolve(rel, lock->rel_lock);
	if (likely(pass))
	{
		// success
		rel->lock_set[lock->rel_lock]++;
	} else
	{
		// add lock to the relation wait list
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
lock_mgr_lock(LockMgr*    self, Relation* rel, LockId rel_lock,
              Str*        name,
              const char* func,
              int         func_line)
{
	if (unlikely(rel_lock == LOCK_NONE))
		return NULL;

	// create lock
	auto lock = lock_allocate(rel, rel_lock, name, func, func_line);
	lock_mgr_lock_of(self, lock);

	// attach lock to the coroutine (unless detached)
	if (likely(! name))
	{
		lock->coro = am_self();
		list_init(&lock->link);
		list_append(&lock->coro->locks, &lock->link);
	}

	return lock;
}

hot static inline void
lock_mgr_lock_access(LockMgr*    self, Access* access,
                     const char* func,
                     int         func_line)
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
		lock_mgr_lock(self, record->rel, record->lock, NULL, func, func_line);
	}
}

hot static inline void
lock_mgr_unlock(LockMgr* self, Lock* lock)
{
	if (!lock || lock->rel_lock == LOCK_NONE)
		return;

	auto rel = lock->rel;
	spinlock_lock(&rel->lock);

	// detach from the relation lock set
	rel->lock_set[lock->rel_lock]--;

	// detach from the lock manager
	lock_mgr_remove(self, lock);

	// batch wakeup non-conflicting waiters
	while (rel->lock_wait_count > 0)
	{
		auto lock = container_of(list_first(&rel->lock_wait), Lock, link);
		if (lock_resolve(lock->rel, lock->rel_lock))
		{
			rel->lock_set[lock->rel_lock]++;
			rel->lock_wait_count--;
			list_unlink(&lock->link);
			event_signal(&lock->event);
			continue;
		}
		break;
	}

	spinlock_unlock(&lock->rel->lock);

	lock->rel      = NULL;
	lock->rel_lock = LOCK_NONE;

	// detach from the coroutine
	if (lock->coro)
	{
		assert(lock->coro == am_self());
		lock->coro = NULL;
		list_unlink(&lock->link);
	}
	lock_free(lock);
}

hot static inline Lock*
lock_mgr_find(LockMgr* self, Str* name)
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
lock_mgr_list(LockMgr* self, Buf* buf)
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
