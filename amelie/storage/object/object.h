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

typedef struct Object Object;

struct Object
{
	Id      id;
	int     state;
	Meta    meta;
	Buf     meta_data;
	File    file;
	Source* source;
	List    link;
};

Object*
object_allocate(Source*, Id*);
void object_free(Object*);
void object_open(Object*, int, bool);
void object_create(Object*, int);
void object_delete(Object*, int);
void object_rename(Object*, int, int);

static inline void
object_set(Object* self, int state)
{
	self->state |= state;
}

static inline void
object_unset(Object* self, int state)
{
	self->state &= ~state;
}

static inline bool
object_has(Object* self, int state)
{
	return (self->state & state) > 0;
}

hot always_inline static inline Row*
object_min(Object* self)
{
	return meta_region_min(meta_min(&self->meta, &self->meta_data));
}

hot always_inline static inline Row*
object_max(Object* self)
{
	return meta_region_max(meta_max(&self->meta, &self->meta_data));
}
