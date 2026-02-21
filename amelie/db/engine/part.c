
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>

Part*
part_allocate(Id* id, Sequence* seq, bool unlogged)
{
	auto self = (Part*)am_malloc(sizeof(Part));
	self->indexes       = NULL;
	self->indexes_count = 0;
	self->heap          = heap_allocate(false);
	self->heap_shadow   = NULL;
	self->seq           = seq;
	self->unlogged      = unlogged;
	id_init(&self->id);
	id_set_free(&self->id, (IdFree)part_free);
	id_copy(&self->id, id);
	track_init(&self->track);
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
	if (self->heap)
		heap_free(self->heap);
	if (self->heap_shadow)
		heap_free(self->heap_shadow);
	am_free(self);
}

void
part_open(Part* self)
{
	// read heap file
	heap_open(self->heap, &self->id, ID_RAM);

	// rebuild indexes
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, self->heap, NULL);
	uint64_t count = 0;
	while (heap_iterator_has(&it))
	{
		auto row = heap_iterator_at(&it);
		part_apply(self, row, false);
		count++;
		heap_iterator_next(&it);
	}

	auto total = (double)page_mgr_used(&self->heap->page_mgr) / 1024 / 1024;
	auto id = &self->id;
	info("recover: %s/%s/%05" PRIu64 ".ram (%.2f MiB, %" PRIu64 " rows)",
	     id->storage->storage->config->name.pos,
	     id->tier->name.pos,
	     id->id,
	     total, count);
}

void
part_truncate(Part* self)
{
	for (auto index = self->indexes; index; index = index->next)
		index_truncate(index);
}

void
part_index_add(Part* self, Index* index)
{
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
part_index_create(Part* self, IndexConfig* config)
{
	Index* index;
	if (config->type == INDEX_TREE)
		index = index_tree_allocate(config, self);
	else
	if (config->type == INDEX_HASH)
		index = index_hash_allocate(config, self);
	else
		error("unrecognized index type");

	part_index_add(self, index);
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

void
part_status(Part* self, Buf* buf, bool extended)
{
	auto heap = self->heap->header;
	id_status(&self->id, buf, extended,
	          heap->hash_min,
	          heap->hash_max,
	          heap->lsn,
	          page_mgr_used(&self->heap->page_mgr),
	          heap->compression);
}
