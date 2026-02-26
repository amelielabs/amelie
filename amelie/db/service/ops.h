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

typedef struct OpsPart OpsPart;
typedef struct OpsLock OpsLock;
typedef struct Ops     Ops;

struct OpsPart
{
	uint64_t id;
	Relation rel;
	Str      rel_name;
	char     rel_id[32];
	int      refs;
	List     link;
};

struct OpsLock
{
	Lock*    lock;
	Lock*    lock_catalog;
	OpsPart* part;
	Ops*     ops;
};

struct Ops
{
	Spinlock lock;
	List     list;
	List     list_free;
};

static inline OpsPart*
ops_part_allocate(void)
{
	auto self = (OpsPart*)am_malloc(sizeof(OpsPart));
	memset(self, 0, sizeof(OpsPart));
	relation_init(&self->rel);
	relation_set_name(&self->rel, &self->rel_name);
	list_init(&self->link);
	return self;
}

static inline void
ops_part_free(OpsPart* self)
{
	relation_free(&self->rel);
	am_free(self);
}

static inline void
ops_part_set(OpsPart* self, uint64_t id)
{
	self->id = id;
	auto size = sfmt(self->rel_id, sizeof(self->rel_id), "part_%" PRIu64, id);
	str_set(&self->rel_name, self->rel_id, size);
}

static inline void
ops_lock_init(OpsLock* self)
{
	self->lock         = NULL;
	self->lock_catalog = NULL;
	self->part         = NULL;
	self->ops          = NULL;
}

static inline void
ops_init(Ops* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	list_init(&self->list_free);
}

static inline void
ops_free(Ops* self)
{
	list_foreach_safe(&self->list_free)
	{
		auto part = list_at(OpsPart, link);
		ops_part_free(part);
	}
	list_init(&self->list_free);
	spinlock_free(&self->lock);
}

static inline OpsPart*
ops_create(Ops* self, uint64_t id)
{
	// find existing partition relation
	list_foreach(&self->list)
	{
		auto ref = list_at(OpsPart, link);
		if (ref->id == id)
		{
			ref->refs++;
			return ref;
		}
	}

	// create a new one
	OpsPart* part = NULL;
	if (! list_empty(&self->list_free))
	{
		part = container_of(list_pop(&self->list_free), OpsPart, link);
		list_init(&part->link);
	} else {
		part = ops_part_allocate();
	}
	ops_part_set(part, id);
	list_append(&self->list, &part->link);
	part->refs++;
	return part;
}

hot static inline void
ops_lock(Ops* self, OpsLock* lock, uint64_t id)
{
	// lock catalog (shared)
	lock->ops          = self;
	lock->lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);

	// find existing or create a new partition relation for locking
	spinlock_lock(&self->lock);
	lock->part = ops_create(self, id);
	spinlock_unlock(&self->lock);

	// lock partition
	lock->lock = lock(&lock->part->rel, LOCK_EXCLUSIVE);
}

hot static inline void
ops_unlock(OpsLock* lock)
{
	auto self = lock->ops;
	if (! self)
		return;

	// unlock partition
	if (lock->lock)
	{
		unlock(lock->lock);
		lock->lock = NULL;
	}

	// unref partition and move to the free list
	auto part = lock->part;
	if (part)
	{
		spinlock_lock(&self->lock);

		part->refs--;
		if (! part->refs)
		{
			list_unlink(&part->link);
			list_init(&part->link);
			list_append(&self->list_free, &part->link);
		}

		spinlock_unlock(&self->lock);
		lock->part = NULL;
	}

	// unlock catalog
	if (lock->lock_catalog)
	{
		unlock(lock->lock_catalog);
		lock->lock_catalog = NULL;
	}

	lock->ops = NULL;
}
