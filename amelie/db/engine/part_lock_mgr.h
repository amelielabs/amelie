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

typedef struct PartLock    PartLock;
typedef struct PartLockMgr PartLockMgr;

struct PartLock
{
	uint64_t  id;
	Event     event;
	Lock*     lock;
	PartLock* waiter;
	List      link;
};

struct PartLockMgr
{
	Spinlock lock;
	List     list_locks;
};

static inline void
part_lock_init(PartLock* self)
{
	self->id     = 0;
	self->lock   = NULL;
	self->waiter = NULL;
	event_init(&self->event);
	list_init(&self->link);
}

static inline void
part_lock_mgr_init(PartLockMgr* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list_locks);
}

static inline PartLock*
part_lock_mgr_find(PartLockMgr* self, uint64_t id)
{
	list_foreach(&self->list_locks)
	{
		auto lock = list_at(PartLock, link);
		if (lock->id == id)
			return lock;
	}
	return NULL;
}

hot static inline void
part_lock_mgr_lock(PartLockMgr* self, PartLock* lock, uint64_t id)
{
	lock->id = id;
	event_attach(&lock->event);

	spinlock_lock(&self->lock);

	// find existing lock
	auto last = part_lock_mgr_find(self, id);
	if (! last)
	{
		list_append(&self->list_locks, &lock->link);
		spinlock_unlock(&self->lock);
		return;
	}

	// join the wait chain
	for (; last->waiter; last = last->waiter);
	last->waiter = lock;
	spinlock_unlock(&self->lock);

	// wait
	event_wait(&lock->event, -1);

	// (always head after resume)
}

hot static inline void
part_lock_mgr_unlock(PartLockMgr* self, PartLock* lock)
{
	spinlock_lock(&self->lock);

	// always head
	list_unlink(&lock->link);

	// pass lock to the next waiter
	auto waiter = lock->waiter;
	if (waiter)
		list_append(&self->list_locks, &waiter->link);

	spinlock_unlock(&self->lock);

	// wakeup next waiter
	if (waiter)
		event_signal(&waiter->event);

	part_lock_init(lock);
}
