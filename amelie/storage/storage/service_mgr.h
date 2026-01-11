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

typedef struct ServiceLock ServiceLock;
typedef struct ServiceMgr  ServiceMgr;

struct ServiceLock
{
	Id*          id;
	Event        event;
	ServiceLock* waiter;
	List         link;
};

struct ServiceMgr
{
	Spinlock lock;
	List     list_locks;
};

static inline void
service_lock_init(ServiceLock* self)
{
	self->id     = NULL;
	self->waiter = NULL;
	event_init(&self->event);
	list_init(&self->link);
}

static inline void
service_mgr_init(ServiceMgr* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list_locks);
}

static inline ServiceLock*
service_mgr_find(ServiceMgr* self, uint64_t psn)
{
	list_foreach(&self->list_locks)
	{
		auto lock = list_at(ServiceLock, link);
		if (lock->id->id == psn)
			return lock;
	}
	return NULL;
}

hot static inline void
service_mgr_lock(ServiceMgr* self, ServiceLock* lock, Id* id)
{
	lock->id = id;
	event_attach(&lock->event);

	spinlock_lock(&self->lock);

	// find existing lock
	auto last = service_mgr_find(self, lock->id->id);
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
service_mgr_unlock(ServiceMgr* self, ServiceLock* lock)
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

	service_lock_init(lock);
}
