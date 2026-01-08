
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
part_allocate(Source*   source,
              Id*       id,
              Sequence* seq,
              bool      unlogged)
{
	auto self = (Part*)am_malloc(sizeof(Part));
	self->id            = *id;
	self->source        = source;
	self->indexes_count = 0;
	self->heap          = &self->heap_a;
	self->volume        = NULL;
	self->object        = NULL;
	self->seq           = seq;
	self->unlogged      = unlogged;
	track_init(&self->track);
	list_init(&self->indexes);
	heap_init(&self->heap_a);
	heap_init(&self->heap_b);
	heap_create(&self->heap_a);
	list_init(&self->link_volume);
	list_init(&self->link);
	rbtree_init_node(&self->link_range);
	hashtable_node_init(&self->link_hash);
	return self;
}

void
part_free(Part* self)
{
	track_free(&self->track);
	if (self->object)
		object_free(self->object);
	list_foreach_safe(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_free(index);
	}
	heap_free(&self->heap_a);
	heap_free(&self->heap_b);
	am_free(self);
}

void
part_load(Part* self)
{
	// open partition object file
	auto object = object_allocate(self->source, &self->id);
	defer(object_free, object);
	object_open(object, ID, true);

	// iterate object file
	ObjectIterator it;
	object_iterator_init(&it);
	defer(object_iterator_free, &it);

	// read into heap
	auto count = 0ul;
	auto keys  = index_keys(part_primary(self));
	object_iterator_open(&it, keys, object, NULL);
	while (object_iterator_has(&it))
	{
		auto ref = object_iterator_at(&it);
		auto row = row_copy(self->heap, ref);
		part_ingest(self, row);
		count++;
		object_iterator_next(&it);
	}

	char uuid[UUID_SZ];
	uuid_get(&self->id.id_table, uuid, sizeof(uuid));

	auto total = (double)object->file.size / 1024 / 1024;
	info(" %s/%05" PRIu64 ".%05" PRIu64 " (%.2f MiB, %" PRIu64 " rows)",
	     uuid,
	     self->id.id_parent,
	     self->id.id,
	     total, count);
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
