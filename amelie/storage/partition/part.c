
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
#include <amelie_object.h>
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
	self->indexes       = NULL;
	self->indexes_count = 0;
	self->heap          = heap_allocate();
	self->heap_shadow   = heap_allocate();
	self->volume        = NULL;
	self->object        = NULL;
	self->seq           = seq;
	self->unlogged      = unlogged;
	heap_create(self->heap);
	track_init(&self->track);
	list_init(&self->link_volume);
	list_init(&self->link);
	rbtree_init_node(&self->link_range);
	return self;
}

void
part_free(Part* self)
{
	track_free(&self->track);
	auto index = self->indexes;
	while (index)
	{
		auto next = index->next;
		index_free(index);
		index = next;
	}
	heap_free(self->heap);
	heap_free(self->heap_shadow);
	if (self->object)
		object_free(self->object);
	am_free(self);
}

void
part_load(Part* self)
{
	auto object = self->object;
	auto keys = index_keys(part_primary(self));

	// iterate object file
	ObjectIterator it;
	object_iterator_init(&it);
	defer(object_iterator_free, &it);
	object_iterator_open(&it, keys, object, NULL);

	// read into heap
	auto count = 0ul;
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
	for (auto index = self->indexes; index; index = index->next)
		index_truncate(index);
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

	// link index
	auto tail = self->indexes;
	while (tail && tail->next)
		tail = tail->next;
	if (tail)
		tail->next = index;
	else
		self->indexes = index;
	self->indexes_count++;
}

void
part_index_drop(Part* self, Str* name)
{
	auto index = self->indexes;
	auto tail  = (Index*)NULL;
	for (; index; index = index->next)
	{
		if (str_compare_case(&index->config->name, name))
			break;
		tail = index;
	}
	if (! index)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));

	// unlink index
	if (tail)
		tail->next = index->next;
	else
		self->indexes = index->next;
	self->indexes_count--;
	index_free(index);
}

Index*
part_index_find(Part* self, Str* name, bool error_if_not_exists)
{
	for (auto index = self->indexes; index; index = index->next)
	{
		if (str_compare_case(&index->config->name, name))
			return index;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}
