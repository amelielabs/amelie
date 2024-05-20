#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Handle Handle;

typedef void (*HandleFree)(Handle*);

struct Handle
{
	Str*       schema;
	Str*       name;
	Uuid*      id;
	uint64_t   lsn;
	HandleFree free_function;
	List       link;
};

static inline void
handle_init(Handle* self)
{
	self->schema        = NULL;
	self->name          = NULL;
	self->id            = NULL;
	self->lsn           = 0;
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
handle_set_id(Handle* self, Uuid* id)
{
	self->id = id;
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
