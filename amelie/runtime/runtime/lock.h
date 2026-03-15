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
	Rel*        rel;
	LockId      rel_lock;
	int         refs;
	bool        waiting;
	Event       event;
	Str         name;
	Coroutine*  coro;
	const char* func;
	List        link_mgr;
	List        link;
};

static inline void
lock_init(Lock* self, Rel* rel, LockId rel_lock,
          Str*  name, const char* func)
{
	self->rel       = rel;
	self->rel_lock  = rel_lock;
	self->refs      = 0;
	self->waiting   = false;
	self->coro      = NULL;
	self->func      = func;
	str_init(&self->name);
	if (unlikely(name))
		str_copy(&self->name, name);
	event_init(&self->event);
	list_init(&self->link_mgr);
	list_init(&self->link);
}

static inline void
lock_free(Lock* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
lock_reset(Lock* self)
{
	str_free(&self->name);
}

static inline void
lock_attach(Lock* self)
{
	assert(! self->coro);
	self->coro = am_self();
	list_append(&self->coro->locks, &self->link);
}

static inline void
lock_detach(Lock* self)
{
	assert(self->coro == am_self());
	self->coro = NULL;
	list_unlink(&self->link);
}

static inline void
lock_write(Lock* self, Buf* buf)
{
	// {}
	encode_obj(buf);

	// name
	if (! str_empty(&self->name))
	{
		encode_raw(buf, "name", 4);
		encode_string(buf, &self->name);
	}

	// relation
	encode_raw(buf, "relation", 8);
	encode_string(buf, self->rel->name);

	// lock
	encode_raw(buf, "lock", 4);
	lock_id_encode(buf, self->rel_lock);

	// waiting
	encode_raw(buf, "waiting", 7);
	encode_bool(buf, self->waiting);

	// function
	encode_raw(buf, "function", 8);
	encode_cstr(buf, self->func);

	encode_obj_end(buf);
}
