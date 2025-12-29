
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

Part*
part_allocate(PartConfig* config, Source* source,
              Sequence*   seq,
              bool        unlogged)
{
	auto self = (Part*)am_malloc(sizeof(Part));
	self->indexes_count = 0;
	self->heap          = &self->heap_a;
	self->config        = part_config_copy(config);
	self->source        = source;
	self->seq           = seq;
	self->unlogged      = unlogged;
	track_init(&self->track);
	list_init(&self->indexes);
	heap_init(&self->heap_a);
	heap_init(&self->heap_b);
	heap_create(&self->heap_a);
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
	part_config_free(self->config);
	am_free(self);
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
part_index_add(Part* self, IndexConfig* config)
{
	Index* index;
	if (config->type == INDEX_TREE)
		index = index_tree_allocate(config, self);
	else
	if (config->type == INDEX_HASH)
		index = index_hash_allocate(config, self);
	else
		error("unrecognized index type");
	list_append(&self->indexes, &index->link);
	self->indexes_count++;
}

void
part_index_drop(Part* self, Str* name)
{
	auto index = part_index_find(self, name, true);
	list_unlink(&index->link);
	self->indexes_count--;
	index_free(index);
}

Index*
part_index_find(Part* self, Str* name, bool error_if_not_exists)
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
