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

typedef struct Branch Branch;

struct Branch
{
	Meta    meta;
	Buf     meta_data;
	File*   object_file;
	Id*     object_id;
	Branch* next;
};

static inline Branch*
branch_allocate(Id* object_id, File* object_file)
{
	auto self = (Branch*)am_malloc(sizeof(Branch));
	self->object_id   = object_id;
	self->object_file = object_file;
	self->next        = NULL;
	meta_init(&self->meta);
	buf_init(&self->meta_data);
	return self;
}

static inline void
branch_free(Branch* self)
{
	buf_free(&self->meta_data);
	am_free(self);
}

hot always_inline static inline Row*
branch_min(Branch* self)
{
	if (unlikely(! self->meta.regions))
		return NULL;
	return meta_region_min(meta_min(&self->meta, &self->meta_data));
}

hot always_inline static inline Row*
branch_max(Branch* self)
{
	if (unlikely(! self->meta.regions))
		return NULL;
	return meta_region_max(meta_max(&self->meta, &self->meta_data));
}

static inline void
branch_status(Branch* self, Buf* buf, int flags)
{
	unused(flags);
	encode_obj(buf);

	// min
	encode_raw(buf, "min", 3);
	encode_integer(buf, 0);

	// max
	encode_raw(buf, "max", 3);
	encode_integer(buf, 0);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, 0);

	// size
	encode_raw(buf, "size", 4);
	encode_integer(buf, self->meta.size_total);

	// compression
	encode_raw(buf, "compression", 11);
	encode_integer(buf, self->meta.compression);

	encode_obj_end(buf);
}
