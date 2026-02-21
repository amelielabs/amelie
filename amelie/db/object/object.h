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
typedef struct Tier   Tier;

struct Object
{
	Id         id;
	Meta       meta;
	Buf        meta_data;
	File       file;
	RbtreeNode link_mapping;
};

Object* object_allocate(Id*);
void    object_free(Object*);
void    object_open(Object*, int, bool);
void    object_create(Object*, int);
void    object_delete(Object*, int);
void    object_rename(Object*, int, int);
void    object_status(Object*, Buf*, bool);

hot always_inline static inline Row*
object_min(Object* self)
{
	if (unlikely(! self->meta.regions))
		return NULL;
	return meta_region_min(meta_min(&self->meta, &self->meta_data));
}

hot always_inline static inline Row*
object_max(Object* self)
{
	if (unlikely(! self->meta.regions))
		return NULL;
	return meta_region_max(meta_max(&self->meta, &self->meta_data));
}

static inline Object*
object_of(Id* self)
{
	return (Object*)self;
}
