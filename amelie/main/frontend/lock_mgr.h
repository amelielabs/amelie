#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct LockMgr LockMgr;

struct LockMgr
{
	Resource  rw;
	Resource  rw_ref;
	Resource  checkpoint;
	Lock*     lock_exclusive;
	Lock*     lock_exclusive_ref;
	Lock*     lock_checkpoint;
	LockCache lock_cache;
};

static inline void
lock_mgr_init(LockMgr* self)
{
	self->lock_exclusive     = NULL;
	self->lock_exclusive_ref = NULL;
	self->lock_checkpoint    = NULL;
	lock_cache_init(&self->lock_cache);
	resource_init(&self->rw, &self->lock_cache);
	resource_init(&self->rw_ref, &self->lock_cache);
	resource_init(&self->checkpoint, &self->lock_cache);
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
		auto lock = resource_lock(&self->rw, false);
		assert(! self->lock_exclusive);
		self->lock_exclusive = lock;
		break;
	}
	case LOCK_REF:
		lock_shared = resource_lock(&self->rw_ref, true);
		break;
	case LOCK_REF_EXCLUSIVE:
	{
		auto lock = resource_lock(&self->rw_ref, false);
		assert(! self->lock_exclusive_ref);
		self->lock_exclusive_ref = lock;
		break;
	}
	case LOCK_CHECKPOINT:
	{
		// exclusive only
		auto lock = resource_lock(&self->checkpoint, false);
		assert(! self->lock_checkpoint);
		self->lock_checkpoint = lock;
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
	case LOCK_REF:
		assert(lock);
		resource_unlock(lock);
		break;
	case LOCK_REF_EXCLUSIVE:
		assert(self->lock_exclusive_ref);
		resource_unlock(self->lock_exclusive_ref);
		self->lock_exclusive_ref = NULL;
		break;
	case LOCK_CHECKPOINT:
		assert(self->lock_checkpoint);
		resource_unlock(self->lock_checkpoint);
		self->lock_checkpoint = NULL;
		break;
	case LOCK_NONE:
		break;
	}
}
