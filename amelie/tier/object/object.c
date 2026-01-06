
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_object.h>

Object*
object_allocate(Source* source, Id* id)
{
	auto self = (Object*)am_malloc(sizeof(Object));
	self->id      = *id;
	self->state   = ID_NONE;
	self->source  = source;
	meta_init(&self->meta);
	buf_init(&self->meta_data);
	file_init(&self->file);
	list_init(&self->link);
	return self;
}

void
object_free(Object* self)
{
	file_close(&self->file);
	buf_free(&self->meta_data);
	am_free(self);
}

void
object_open(Object* self, int state, bool read_index)
{
	switch (state) {
	// <source_path>/<table_uuid>/<id_parent>.<id>
	case ID:
	{
		object_file_open(&self->file, self->source, &self->id, state, &self->meta);
		if (read_index)
			object_file_read(&self->file, self->source,
			                 &self->meta,
			                 &self->meta_data, false);
		break;
	}
	default:
		abort();
		break;
	}
}

void
object_create(Object* self, int state)
{
	object_file_create(&self->file, self->source, &self->id, state);
}

void
object_delete(Object* self, int state)
{
	object_file_delete(self->source, &self->id, state);
}

void
object_rename(Object* self, int from, int to)
{
	object_file_rename(self->source, &self->id, from, to);
}
