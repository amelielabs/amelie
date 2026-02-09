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

typedef enum
{
	LOCK_CATALOG,
	LOCK_DDL,
	LOCK_CATEGORY_MAX
} LockCategory;

struct LockMgr
{
	Spinlock lock;
	List     list;
	List     list_wait;
	Relation rels[LOCK_CATEGORY_MAX];
};

static inline void
lock_mgr_init(LockMgr* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	list_init(&self->list_wait);
	for (auto i = 0; i < LOCK_CATEGORY_MAX; i++)
		relation_init(&self->rels[i]);
}

static inline void
lock_mgr_free(LockMgr* self)
{
	spinlock_free(&self->lock);
}

hot static inline Lock*
lock_mgr_lock(LockMgr* self, Relation* rel, LockId rel_lock)
{
	if (unlikely(rel_lock == LOCK_NONE))
		return NULL;

	// create lock
	auto lock  = lock_allocate(rel, rel_lock);
	auto event = &lock->event;

	// try to lock the relation
	spinlock_lock(&self->lock);

	for (;;)
	{
		if (likely(lock_resolve(rel, rel_lock)))
		{
			// lock relation and attach the lock
			lock_set(lock);
			list_append(&self->list, &lock->link_mgr);
			break;
		}

		event_init(event);
		event_attach(event);

		// add lock to the wait list
		list_append(&self->list_wait, &lock->link);
		spinlock_unlock(&self->lock);

		// wait
		event_wait(event, -1);

		// retry
		spinlock_lock(&self->lock);
	}

	spinlock_unlock(&self->lock);
	return lock;
}

hot static inline Lock*
lock_mgr_lock_access(LockMgr* self, Access* access)
{
	// do nothing if the access list is empty
	if (access_empty(access))
		return NULL;

	// create lock
	auto lock  = lock_allocate_access(access);
	auto event = &lock->event;

	// iterate and try to lock relations
	spinlock_lock(&self->lock);

	for (;;)
	{
		// resolve conflicts
		auto pass = false;
		for (auto i = 0; i < access->list_count; i++)
		{
			auto record = access_at(access, i);
			if (! (pass = lock_resolve(record->rel, record->lock)))
				break;
		}

		// success
		if (likely(pass))
		{
			// lock relations and attach the lock
			lock_set(lock);
			list_append(&self->list, &lock->link_mgr);
			break;
		}

		event_init(event);
		event_attach(event);

		// add lock to the wait list
		list_append(&self->list_wait, &lock->link);
		spinlock_unlock(&self->lock);

		// wait
		event_wait(event, -1);

		// retry
		spinlock_lock(&self->lock);
	}

	spinlock_unlock(&self->lock);
	return lock;
}

hot static inline void
lock_mgr_unlock(LockMgr* self, Lock* lock)
{
	if (!lock || lock->type == LOCK_TYPE_NONE)
		return;

	spinlock_lock(&self->lock);

	// unlock relations and detach the lock
	lock_unset(lock);
	list_unlink(&lock->link_mgr);

	// wakeup waiters
	list_foreach(&self->list_wait)
	{
		auto ref = list_at(Lock, link);
		// todo: wakeup only related locks
		list_unlink(&ref->link);
		event_signal(&ref->event);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
lock_mgr_unlock_list(LockMgr* self, List* list)
{
	if (list_empty(list))
		return;

	spinlock_lock(&self->lock);

	// unlock relations and detach the lock
	list_foreach_reverse_safe(list)
	{
		auto lock = list_at(Lock, link);
		lock_unset(lock);
		list_unlink(&lock->link_mgr);
	}

	// wakeup waiters
	list_foreach(&self->list_wait)
	{
		auto ref = list_at(Lock, link);
		// todo: wakeup only related locks
		list_unlink(&ref->link);
		event_signal(&ref->event);
	}

	spinlock_unlock(&self->lock);
}
