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
	Event      event;
	bool       access_lock;
	Coroutine* coro;
	union
	{
		Access* access;
		struct {
			Relation* rel;
			LockId    rel_lock;
		};
	};
	List       link;
	List       link_mgr;
};

static inline void
lock_init(Lock* self)
{
	memset(self, 0, sizeof(*self));
	list_init(&self->link);
	list_init(&self->link_mgr);
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

static inline Lock*
lock_allocate_access(Access* access)
{
	auto self = (Lock*)palloc(sizeof(Lock));
	lock_init(self);
	self->access_lock = true;
	self->access      = access;
	return self;
}

hot static inline void
lock_set(Lock* self)
{
	// attach to the coroutine
	assert(! self->coro);
	self->coro = am_self();
	list_append(&self->coro->locks, &self->link);

	// single relation
	if (! self->access_lock)
	{
		self->rel->lock[self->rel_lock]++;
		return;
	}

	// relations from the access list
	auto access = self->access;
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		record->rel->lock[record->lock]++;
	}
}

hot static inline void
lock_unset(Lock* self)
{
	// detach from coroutine
	assert(self->coro == am_self());
	self->coro = NULL;
	list_unlink(&self->link);

	// single relation
	if (! self->access_lock)
	{
		self->rel->lock[self->rel_lock]--;
		return;
	}

	// relations from the access list
	auto access = self->access;
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		record->rel->lock[record->lock]--;
	}
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
