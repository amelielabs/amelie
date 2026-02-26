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

typedef struct ServiceRel     ServiceRel;
typedef struct ServiceLock    ServiceLock;
typedef struct ServiceLockMgr ServiceLockMgr;

struct ServiceRel
{
	uint64_t id;
	Relation rel;
	Str      rel_name;
	char     rel_id[32];
	int      refs;
	List     link;
};

struct ServiceLock
{
	Lock*           lock;
	Lock*           lock_catalog;
	ServiceRel*     rel;
	ServiceLockMgr* ops;
};

struct ServiceLockMgr
{
	Spinlock lock;
	List     list;
	List     list_free;
};

static inline ServiceRel*
service_rel_allocate(void)
{
	auto self = (ServiceRel*)am_malloc(sizeof(ServiceRel));
	memset(self, 0, sizeof(ServiceRel));
	relation_init(&self->rel);
	relation_set_name(&self->rel, &self->rel_name);
	list_init(&self->link);
	return self;
}

static inline void
service_rel_free(ServiceRel* self)
{
	relation_free(&self->rel);
	am_free(self);
}

static inline void
service_rel_set(ServiceRel* self, uint64_t id)
{
	self->id = id;
	auto size = sfmt(self->rel_id, sizeof(self->rel_id), "service_%" PRIu64, id);
	str_set(&self->rel_name, self->rel_id, size);
}

static inline void
service_lock_init(ServiceLock* self)
{
	self->lock         = NULL;
	self->lock_catalog = NULL;
	self->rel          = NULL;
	self->ops          = NULL;
}

static inline void
service_lock_mgr_init(ServiceLockMgr* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	list_init(&self->list_free);
}

static inline void
service_lock_mgr_free(ServiceLockMgr* self)
{
	list_foreach_safe(&self->list_free)
	{
		auto rel = list_at(ServiceRel, link);
		service_rel_free(rel);
	}
	list_init(&self->list_free);
	spinlock_free(&self->lock);
}

static inline ServiceRel*
service_lock_mgr_create(ServiceLockMgr* self, uint64_t id)
{
	// find existing relation
	list_foreach(&self->list)
	{
		auto ref = list_at(ServiceRel, link);
		if (ref->id == id)
		{
			ref->refs++;
			return ref;
		}
	}

	// create a new one
	ServiceRel* rel = NULL;
	if (! list_empty(&self->list_free))
	{
		rel = container_of(list_pop(&self->list_free), ServiceRel, link);
		list_init(&rel->link);
	} else {
		rel = service_rel_allocate();
	}
	service_rel_set(rel, id);
	list_append(&self->list, &rel->link);
	rel->refs++;
	return rel;
}

hot static inline void
service_lock_mgr_lock(ServiceLockMgr* self, ServiceLock* lock, uint64_t id)
{
	// lock catalog (shared)
	lock->ops          = self;
	lock->lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);

	// find existing or create a new relation for locking
	spinlock_lock(&self->lock);
	lock->rel = service_lock_mgr_create(self, id);
	spinlock_unlock(&self->lock);

	// lock relation
	lock->lock = lock(&lock->rel->rel, LOCK_EXCLUSIVE);
}

hot static inline void
service_lock_mgr_unlock(ServiceLock* lock)
{
	auto self = lock->ops;
	if (! self)
		return;

	// unlock relation
	if (lock->lock)
	{
		unlock(lock->lock);
		lock->lock = NULL;
	}

	// unref and move to the free list
	auto rel = lock->rel;
	if (rel)
	{
		spinlock_lock(&self->lock);

		rel->refs--;
		if (! rel->refs)
		{
			list_unlink(&rel->link);
			list_init(&rel->link);
			list_append(&self->list_free, &rel->link);
		}

		spinlock_unlock(&self->lock);
		lock->rel = NULL;
	}

	// unlock catalog
	if (lock->lock_catalog)
	{
		unlock(lock->lock_catalog);
		lock->lock_catalog = NULL;
	}

	lock->ops = NULL;
}
