#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Handle Handle;

typedef void (*HandleFree)(Handle*);

struct Handle
{
	Str*       name;
	uint64_t   lsn;
	HandleFree free_function;
	List       link;
};

static inline void
handle_init(Handle* self)
{
	self->name          = NULL;
	self->lsn           = 0;
	self->free_function = NULL;
	list_init(&self->link);
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
