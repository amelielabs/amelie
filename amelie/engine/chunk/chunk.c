
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
#include <amelie_chunk.h>

Chunk*
chunk_allocate(Source* source, Id* id)
{
	auto self = (Chunk*)am_malloc(sizeof(Chunk));
	self->id      = *id;
	self->state   = ID_NONE;
	self->refresh = false;
	self->source  = source;
	span_init(&self->span);
	buf_init(&self->span_data);
	file_init(&self->file);
	return self;
}

void
chunk_free(Chunk* self)
{
	file_close(&self->file);
	buf_free(&self->span_data);
	am_free(self);
}

void
chunk_open(Chunk* self, int state, bool read_index)
{
	switch (state) {
	// <source_path>/<uuid>/<id>
	case ID:
	{
		// open and validate file
		span_open(&self->file, self->source, &self->id, state, &self->span);
		if (read_index)
			span_read(&self->file, self->source, &self->span,
			          &self->span_data, false);
		break;
	}
	default:
		abort();
		break;
	}
}

void
chunk_create(Chunk* self, int state)
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
chunk_delete(Chunk* self, int state)
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
chunk_rename(Chunk* self, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(&self->id, self->source, from, path_from);
	id_path(&self->id, self->source, to, path_to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}
