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
	Id          id;
	Meta        meta;
	Buf         meta_data;
	Object*     branches;
	int         branches_count;
	ObjectFile* file;
	RbtreeNode  link_mapping;
	Object*     next;
};

static inline Object*
object_of(Id* self)
{
	return (Object*)self;
}

static inline void
object_free(Object* self)
{
	object_file_unref(self->file, false);
	buf_free(&self->meta_data);
	am_free(self);
}

static inline Object*
object_allocate(ObjectFile* file)
{
	auto self = (Object*)am_malloc(sizeof(Object));
	self->branches_count = 0;
	self->branches       = NULL;
	self->file           = file;
	self->next           = NULL;
	object_file_ref(file);
	id_init(&self->id);
	id_set_free(&self->id, (IdFree)object_free);
	meta_init(&self->meta);
	buf_init(&self->meta_data);
	rbtree_init_node(&self->link_mapping);
	return self;
}

static inline void
object_read(Object* self, size_t offset, bool with_index)
{
	object_file_read(self->file,
	                 &self->meta,
	                 &self->meta_data,
	                 offset,
	                 with_index);
}

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

static inline void
object_attach(Object* self, Object* branch)
{
	// ordered by id (highest first)
	auto head = self->branches;
	while (head && head->next && head->id.id > branch->id.id)
		head = head->next;
	if (head)
	{
		head->next = branch;
	} else
	{
		branch = self->branches;
		self->branches = branch;
	}
	self->branches_count++;
}

static inline void
object_status(Object* self, Buf* buf, int flags, Str* tier)
{
	unused(flags);
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id.id);

	// tier
	encode_raw(buf, "tier", 4);
	encode_string(buf, tier);

	// storage
	encode_raw(buf, "storage", 7);
	encode_string(buf, &self->id.volume->storage->config->name);

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
