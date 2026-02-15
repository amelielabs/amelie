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
	Relation*   rel;
	LockId      rel_lock;
	Event       event;
	Str         name;
	Coroutine*  coro;
	const char* func;
	int         func_line;
	List        link_mgr;
	List        link;
};

static inline void
lock_init(Lock*       self, Relation* rel, LockId rel_lock,
          Str*        name,
          const char* func,
          int         func_line)
{
	self->rel       = rel;
	self->rel_lock  = rel_lock;
	self->coro      = NULL;
	self->func      = func;
	self->func_line = func_line;
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
	if (likely(str_empty(&self->name)))
		return;
	// detached lock
	str_free(&self->name);
	am_free(self);
}

static inline Lock*
lock_allocate(Relation*   rel, LockId rel_lock,
              Str*        name,
              const char* func,
              int         func_line)
{
	Lock* self;
	if (unlikely(name))
		self = (Lock*)malloc(sizeof(Lock));
	else
		self = (Lock*)palloc(sizeof(Lock));
	lock_init(self, rel, rel_lock, name, func, func_line);
	return self;
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

	// function
	encode_raw(buf, "function", 8);
	encode_cstr(buf, self->func);

	// line
	encode_raw(buf, "line", 4);
	encode_integer(buf, self->func_line);

	encode_obj_end(buf);
}
