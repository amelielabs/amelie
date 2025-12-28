
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_io.h>

Part*
part_allocate(Source* source, Id* id)
{
	auto self = (Part*)am_malloc(sizeof(Part));
	self->id            = *id;
	self->state         = ID_NONE;
	self->refresh       = false;
	self->indexes_count = 0;
	self->heap          = &self->heap_a;
	self->source        = source;
	list_init(&self->indexes);
	heap_init(&self->heap_a);
	heap_init(&self->heap_b);
	heap_create(&self->heap_a);
	track_init(&self->track);
	span_init(&self->span);
	buf_init(&self->span_data);
	file_init(&self->file);
	list_init(&self->link);
	hashtable_node_init(&self->link_mgr);
	return self;
}

void
part_free(Part* self)
{
	track_free(&self->track);
	list_foreach_safe(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_free(index);
	}
	heap_free(&self->heap_a);
	heap_free(&self->heap_b);
	file_close(&self->file);
	buf_free(&self->span_data);
	am_free(self);
}

void
part_open(Part* self, int state, bool read_index)
{
	switch (state) {
	// <source_path>/<uuid>/<psn>
	case ID:
	{
		// open and validate partition file
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
part_create(Part* self, int state)
{
	// <source_path>/<uuid>/<psn>.incomplete
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
part_delete(Part* self, int state)
{
	// <source_path>/<uuid>/<psn>
	// <source_path>/<uuid>/<psn>.incomplete
	// <source_path>/<uuid>/<psn>.complete
	char path[PATH_MAX];
	id_path(&self->id, self->source, state, path);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

void
part_rename(Part* self, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(&self->id, self->source, from, path_from);
	id_path(&self->id, self->source, to, path_to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}

void
part_offload(Part* self, bool from_storage)
{
	// remove from storage
	if (from_storage)
	{
		// remove data file
		file_close(&self->file);
		part_delete(self, ID);
		return;
	}
	// todo: reserved for cloud
	abort();
}

void
part_truncate(Part* self)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_truncate(index);
	}
}

void
part_add(Part* self, IndexConfig* config)
{
	Index* index;
	if (config->type == INDEX_TREE)
		index = index_tree_allocate(config);
	else
	if (config->type == INDEX_HASH)
		index = index_hash_allocate(config);
	else
		error("unrecognized index type");
	list_append(&self->indexes, &index->link);
	self->indexes_count++;
}

void
part_del(Part* self, IndexConfig* config)
{
	auto index = part_find(self, &config->name, true);
	list_unlink(&index->link);
	self->indexes_count--;
	index_free(index);
}

Index*
part_find(Part* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		if (str_compare_case(&index->config->name, name))
			return index;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}

bool
part_ready(Part* self)
{
	if (self->refresh)
		return false;
	if (! self->source->refresh_wm)
		return false;
	return page_mgr_used(&self->heap->page_mgr) > (size_t)self->source->refresh_wm;
}
