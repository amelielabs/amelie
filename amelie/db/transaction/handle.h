#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Handle Handle;

typedef void (*HandleFree)(Handle*);

struct Handle
{
	Str*       schema;
	Str*       name;
	HandleFree free_function;
	List       link;
};

static inline void
handle_init(Handle* self)
{
	self->schema        = NULL;
	self->name          = NULL;
	self->free_function = NULL;
	list_init(&self->link);
}

static inline void
handle_set_schema(Handle* self, Str* schema)
{
	self->schema = schema;
}

static inline void
handle_set_name(Handle* self, Str* name)
{
	self->name = name;
}

static inline void
handle_set_free_function(Handle* self, HandleFree func)
{
	self->free_function = func;
}

static inline void
handle_free(Handle* self)
{
	self->free_function(self);
}
