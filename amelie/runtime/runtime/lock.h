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

typedef struct Lock Lock;

struct Lock
{
	Relation*  rel;
	LockId     rel_lock;
	Event      event;
	Coroutine* coro;
	List       link;
};

static inline void
lock_init(Lock* self)
{
	self->rel      = NULL;
	self->rel_lock = LOCK_NONE;
	self->coro     = NULL;
	event_init(&self->event);
	list_init(&self->link);
}

static inline Lock*
lock_allocate(Relation* rel, LockId rel_lock)
{
	auto self = (Lock*)palloc(sizeof(Lock));
	lock_init(self);
	self->rel      = rel;
	self->rel_lock = rel_lock;
	return self;
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

hot static inline Lock*
lock(Relation* rel, LockId rel_lock)
{
	if (unlikely(rel_lock == LOCK_NONE))
		return NULL;

	// create lock and attach it to the coroutine list
	auto self = lock_allocate(rel, rel_lock);
	self->coro = am_self();
	list_append(&self->coro->locks, &self->link);

	// try to lock the relation
	spinlock_lock(&rel->lock);

	// always wait if there are waiters
	auto pass = !rel->lock_wait_count && lock_resolve(rel, rel_lock);
	if (likely(pass))
	{
		// success
		rel->lock_set[rel_lock]++;
	} else
	{
		// add lock to the relation wait list
		list_append(&rel->lock_wait, &self->link);
		rel->lock_wait_count++;
	}

	spinlock_unlock(&rel->lock);

	// wait for the lock
	if (unlikely(! pass))
	{
		auto event = &self->event;
		event_attach(event);
		event_wait(event, -1);
	}

	return self;
}

hot static inline void
lock_access(Access* access)
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
		lock(record->rel, record->lock);
	}
}

hot static inline void
unlock(Lock* self)
{
	if (!self || self->rel_lock == LOCK_NONE)
		return;

	auto rel = self->rel;
	spinlock_lock(&rel->lock);

	// detach from the relation lock set
	rel->lock_set[self->rel_lock]--;

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

	spinlock_unlock(&self->rel->lock);

	self->rel      = NULL;
	self->rel_lock = LOCK_NONE;

	// detach from the coroutine
	assert(self->coro == am_self());
	self->coro = NULL;
	list_unlink(&self->link);
}

hot static inline void
unlock_all(void)
{
	// unlock all locks taken by the coroutine
	auto locks = &am_self()->locks;
	list_foreach_reverse_safe(locks)
		unlock(list_at(Lock, link));
	list_init(locks);
}
