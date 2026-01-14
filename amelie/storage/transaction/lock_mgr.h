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

enum
{
	RELATION_CATALOG,
	RELATION_CHECKPOINT,
	RELATION_MAX
};

struct Lock
{
	Event event;
	List  link;
};

struct LockMgr
{
	Spinlock lock;
	List     list_wait;
	Relation rels[RELATION_MAX];
};

static inline void
lock_init(Lock* self)
{
	event_init(&self->event);
	list_init(&self->link);
}

static inline void
lock_mgr_init(LockMgr* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list_wait);
	for (auto i = 0; i < RELATION_MAX; i++)
		relation_init(&self->rels[i]);
}

static inline void
lock_mgr_free(LockMgr* self)
{
	spinlock_free(&self->lock);
}

hot static inline bool
lock_resolve(Relation* rel, LockId lock)
{
	auto current = rel->lock;
	switch (lock) {
	case LOCK_SHARED:
		return !current[LOCK_EXCLUSIVE];
	case LOCK_SHARED_RW:
		return !current[LOCK_EXCLUSIVE] && !current[LOCK_EXCLUSIVE_RO];
	case LOCK_EXCLUSIVE_RO:
		return !current[LOCK_EXCLUSIVE] && !current[LOCK_SHARED_RW];
	case LOCK_EXCLUSIVE:
		return !current[LOCK_EXCLUSIVE]    &&
		       !current[LOCK_SHARED]       &&
		       !current[LOCK_SHARED_RW]    &&
		       !current[LOCK_EXCLUSIVE_RO] &&
		       !current[LOCK_CALL];
	case LOCK_CALL:
		return !current[LOCK_EXCLUSIVE];
	default:
		abort();
	}
	// conflict
	return false;
}

hot static inline void
lock_access(LockMgr* self, Access* access)
{
	// do nothing if the access list is empty
	if (access_empty(access))
		return;

	// prepare lock
	Lock lock;
	lock_init(&lock);
	event_attach(&lock.event);

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
			// apply relational locks
			for (auto i = 0; i < access->list_count; i++)
			{
				auto record = access_at(access, i);
				record->rel->lock[record->lock]++;
			}
			break;
		}

		// add to the wait list
		list_append(&self->list_wait, &lock.link);
		spinlock_unlock(&self->lock);

		// wait
		event_wait(&lock.event, -1);

		// retry
		spinlock_lock(&self->lock);
		list_unlink(&lock.link);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
unlock_access(LockMgr* self, Access* access)
{
	// do nothing if the access list is empty
	if (access_empty(access))
		return;

	spinlock_lock(&self->lock);

	// unlock relations
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		record->rel->lock[record->lock]--;
		assert(record->rel->lock[record->lock] >= 0);
	}

	// wakeup waiters
	list_foreach(&self->list_wait)
	{
		auto ref = list_at(Lock, link);
		event_signal(&ref->event);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
lock(LockMgr* self, Relation* rel, LockId type)
{
	// prepare lock
	Lock lock;
	lock_init(&lock);
	event_attach(&lock.event);

	// try to lock the relation
	spinlock_lock(&self->lock);

	for (;;)
	{
		if (likely(lock_resolve(rel, type)))
		{
			// apply lock
			rel->lock[type]++;
			break;
		}

		// add to the wait list
		list_append(&self->list_wait, &lock.link);
		spinlock_unlock(&self->lock);

		// wait
		event_wait(&lock.event, -1);

		// retry
		spinlock_lock(&self->lock);
		list_unlink(&lock.link);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
unlock(LockMgr* self, Relation* rel, LockId type)
{
	spinlock_lock(&self->lock);

	// unlock relation
	rel->lock[type]--;
	assert(rel->lock[type] >= 0);

	// wakeup waiters
	list_foreach(&self->list_wait)
	{
		auto ref = list_at(Lock, link);
		event_signal(&ref->event);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
lock_catalog(LockMgr* self, LockId type)
{
	lock(self, &self->rels[RELATION_CATALOG], type);
}

hot static inline void
unlock_catalog(LockMgr* self, LockId type)
{
	unlock(self, &self->rels[RELATION_CATALOG], type);
}

hot static inline void
lock_checkpoint(LockMgr* self, LockId type)
{
	lock(self, &self->rels[RELATION_CHECKPOINT], type);
}

hot static inline void
unlock_checkpoint(LockMgr* self, LockId type)
{
	unlock(self, &self->rels[RELATION_CHECKPOINT], type);
}
