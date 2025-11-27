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

typedef void (*RelationFree)(Relation*);

struct Relation
{
	Str*         schema;
	Str*         name;
	RelationFree free_function;
	List         link;
};

static inline void
relation_init(Relation* self)
{
	self->schema        = NULL;
	self->name          = NULL;
	self->free_function = NULL;
	list_init(&self->link);
}

static inline void
relation_set_schema(Relation* self, Str* schema)
{
	self->schema = schema;
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
	self->free_function(self);
}
