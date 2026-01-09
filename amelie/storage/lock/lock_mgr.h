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

typedef struct Lock    Lock;
typedef struct LockMgr LockMgr;

struct Lock
{
	Access* access;
	Event   event;
	int     refs;
	List    link_wait;
	List    link;
};

struct LockMgr
{
	Spinlock lock;
	List     list;
	List     list_wait;
};

static inline void
lock_init(Lock* self)
{
	self->access = NULL;
	self->refs   = 0;
	event_init(&self->event);
	list_init(&self->link_wait);
	list_init(&self->link);
}

static inline void
lock_set(Lock* self, Access* access)
{
	self->access = access;
	if (! event_attached(&self->event))
		event_attach(&self->event);
}

static inline void
lock_mgr_init(LockMgr* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	list_init(&self->list_wait);
}

static inline void
lock_mgr_free(LockMgr* self)
{
	spinlock_free(&self->lock);
}

hot static inline void
lock_mgr_lock(LockMgr* self, Lock* lock)
{
	// reentrance
	if (lock->refs > 0)
	{
		lock->refs++;
		return;
	}

	spinlock_lock(&self->lock);
retry:
	list_foreach(&self->list)
	{
		auto ref = list_at(Lock, link);
		if (access_try(ref->access, lock->access))
			continue;

		// wait on lock
		list_append(&self->list_wait, &lock->link_wait);
		spinlock_unlock(&self->lock);

		event_wait(&lock->event, -1);

		spinlock_lock(&self->lock);
		list_unlink(&lock->link_wait);
		goto retry;
	}
	spinlock_unlock(&self->lock);
	lock->refs = 1;
}

hot static inline void
lock_mgr_unlock(LockMgr* self, Lock* lock)
{
	if (!lock->refs || --lock->refs > 0)
		return;

	// detach and wakeup waiters
	spinlock_lock(&self->lock);

	list_unlink(&lock->link);
	list_foreach(&self->list_wait)
	{
		auto ref = list_at(Lock, link_wait);
		event_signal(&ref->event);
	}

	spinlock_unlock(&self->lock);
}
