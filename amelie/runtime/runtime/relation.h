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

typedef struct Relation Relation;

typedef void (*RelationFree)(Relation*, bool);

struct Relation
{
	Str*         db;
	Str*         name;
	RelationFree free_function;
	Spinlock     lock;
	uint64_t     lock_order;
	int          lock_set[LOCK_MAX];
	List         lock_wait;
	int          lock_wait_count;
	List         link;
};

static inline void
relation_init(Relation* self)
{
	self->db              = NULL;
	self->name            = NULL;
	self->free_function   = NULL;
	self->lock_order      = 0;
	self->lock_wait_count = 0;
	spinlock_init(&self->lock);
	memset(self->lock_set, 0, sizeof(self->lock_set));
	list_init(&self->lock_wait);
	list_init(&self->link);
}

static inline void
relation_set_db(Relation* self, Str* db)
{
	self->db = db;
}

static inline void
relation_set_name(Relation* self, Str* name)
{
	self->name = name;
}

static inline void
relation_set_free_function(Relation* self, RelationFree func)
{
	self->free_function = func;
}

static inline void
relation_set_rsn(Relation* self, uint64_t value)
{
	self->lock_order = value;
}

static inline void
relation_free(Relation* self)
{
	spinlock_free(&self->lock);
	if (self->free_function)
		self->free_function(self, false);
}

static inline void
relation_drop(Relation* self)
{
	spinlock_free(&self->lock);
	if (self->free_function)
		self->free_function(self, true);
}
