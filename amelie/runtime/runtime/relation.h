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
	int          lock[LOCK_MAX];
	RelationFree free_function;
	List         link;
};

static inline void
relation_init(Relation* self)
{
	self->db   = NULL;
	self->name = NULL;
	self->free_function = NULL;
	memset(self->lock, 0, sizeof(self->lock));
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
relation_free(Relation* self)
{
	if (self->free_function)
		self->free_function(self, false);
}

static inline void
relation_drop(Relation* self)
{
	if (self->free_function)
		self->free_function(self, true);
}
