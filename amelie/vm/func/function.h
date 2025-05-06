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

typedef struct Function Function;
typedef struct Call     Call;

typedef void (*FunctionMain)(Call*);

enum
{
	FN_NONE    = 0,
	FN_DERIVE  = 1 << 1,
	FN_CONTEXT = 1 << 2
};

struct Function
{
	Str           schema;
	Str           name;
	int           type;
	int           flags;
	FunctionMain  function;
	List          link;
	HashtableNode link_ht;
};

static inline Function*
function_allocate(int          type,
                  const char*  schema,
                  const char*  name,
                  FunctionMain function)
{
	Function* self = am_malloc(sizeof(Function));
	self->type     = type;
	self->function = function;
	self->flags    = FN_NONE;
	str_init(&self->schema);
	str_init(&self->name);
	str_set_cstr(&self->schema, schema);
	str_set_cstr(&self->name, name);
	list_init(&self->link);
	hashtable_node_init(&self->link_ht);
	return self;
}

static inline void
function_free(Function* self)
{
	am_free(self);
}

static inline void
function_set(Function* self, int flags)
{
	self->flags |= flags;
}
