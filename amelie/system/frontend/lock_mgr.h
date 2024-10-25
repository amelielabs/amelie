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

enum
{
	LOCK_NONE,
	LOCK,
	LOCK_EXCLUSIVE
};

struct LockMgr
{
	Resource  rw;
	Lock*     lock_exclusive;
	LockCache lock_cache;
};

static inline void
lock_mgr_init(LockMgr* self)
{
	self->lock_exclusive = NULL;
	lock_cache_init(&self->lock_cache);
	resource_init(&self->rw, &self->lock_cache);
}

static inline void
lock_mgr_free(LockMgr* self)
{
	lock_cache_free(&self->lock_cache);
}

static inline Lock*
lock_mgr_lock(LockMgr* self, int type)
{
	Lock* lock_shared = NULL;
	switch (type) {
	case LOCK:
		lock_shared = resource_lock(&self->rw, true);
		break;
	case LOCK_EXCLUSIVE:
	{
		assert(! self->lock_exclusive);
		self->lock_exclusive = resource_lock(&self->rw, false);
		break;
	}
	case LOCK_NONE:
		break;
	}
	return lock_shared;
}

static inline void
lock_mgr_unlock(LockMgr* self, int type, Lock* lock)
{
	switch (type) {
	case LOCK:
		assert(lock);
		resource_unlock(lock);
		break;
	case LOCK_EXCLUSIVE:
		assert(self->lock_exclusive);
		resource_unlock(self->lock_exclusive);
		self->lock_exclusive = NULL;
		break;
	case LOCK_NONE:
		break;
	}
}
