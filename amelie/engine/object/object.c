
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
	self->pending = false;
	self->source  = source;
	span_init(&self->index);
	buf_init(&self->index_data);
	file_init(&self->file);
	return self;
}

void
object_free(Object* self)
{
	file_close(&self->file);
	buf_free(&self->index_data);
	am_free(self);
}

void
object_open(Object* self, int state, bool read_index)
{
	switch (state) {
	// <source_path>/<uuid>/<id>
	case ID:
	{
		// open and validate object file
		span_open(&self->file, self->source, &self->id, state, &self->index);
		if (read_index)
			span_read(&self->file, self->source, &self->index,
			          &self->index_data, false);
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
	// <source_path>/<uuid>/<id>.incomplete
	char path[PATH_MAX];
	switch (state) {
	case ID_INCOMPLETE:
	{
		id_path(&self->id, self->source, state, path);
		file_create(&self->file, path);
		break;
	}
	default:
		abort();
		break;
	}
}

void
object_delete(Object* self, int state)
{
	// <source_path>/<uuid>/<id>
	// <source_path>/<uuid>/<id>.incomplete
	// <source_path>/<uuid>/<id>.complete
	char path[PATH_MAX];
	id_path(&self->id, self->source, state, path);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

void
object_rename(Object* self, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(&self->id, self->source, from, path_from);
	id_path(&self->id, self->source, to, path_to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}
