
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>

Part*
part_allocate(PartConfig* config, PartArg* arg)
{
	auto self = (Part*)am_malloc(sizeof(Part));
	self->indexes       = NULL;
	self->indexes_count = 0;
	self->in_progress   = NULL;
	self->heap          = heap_allocate();
	self->config        = part_config_copy(config);
	self->arg           = arg;
	track_init(&self->track);
	flats_init(&self->flats);
	list_init(&self->link_cp);
	list_init(&self->link);
	return self;
}

void
part_free(Part* self)
{
	track_free(&self->track);

	// free index
	IndexOp op;
	index_op_init(&op);
	auto index = self->indexes;
	while (index)
	{
		auto next = index->next;
		index_free(index, &op);
		index = next;
	}

	// free heap, flats and update memory usage
	op.delta -= storage_size(&self->heap->storage);
	op.delta -= flats_size(&self->flats);

	heap_free(self->heap);
	flats_free(&self->flats);
	usage_update(self->arg->memory, op.delta);

	part_config_free(self->config);
	am_free(self);
}

static void
part_open_heap(Part* self, uint64_t checkpoint)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}/{u64}",
	       state_directory(), checkpoint,
	       self->config->id);

	// read heap file
	heap_open(self->heap, path);

	// create primary index iterator for upsert
	auto primary = part_primary(self);
	auto it_upsert = index_iterator(primary);
	defer(iterator_close, it_upsert);

	// create heap iterator
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, self->heap, NULL);

	// build indexes
	uint64_t count = 0;
	for (;; heap_iterator_next(&it))
	{
		auto row = heap_iterator_at(&it);
		if (! row)
			break;

		if (! row->head)
			continue;

		// update index to track the latest version
		IndexOp op =
		{
			.row      = row,
			.row_prev = NULL,
			.it       = it_upsert,
			.delta    = 0
		};
		if (unlikely(index_upsert(primary, &op)))
			abort();
		op.it = NULL;
		for (auto index = primary->next; index; index = index->next)
			index_replace(index, &op);
		usage_update(self->arg->memory, op.delta);
		count++;
	}
	usage_update(self->arg->memory, storage_size(&self->heap->storage));

	auto total = (double)storage_size(&self->heap->storage) / 1024 / 1024;
	info("recover: {u64}/{u64} ({.2f} MiB, {u64} rows)",
	     checkpoint, self->config->id,
	     total, count);
}

static void
part_open_flat(Part* self, Flat* flat, uint64_t checkpoint)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}/{u64}.{d}",
	       state_directory(), checkpoint,
	       self->config->id,
	       flat->column->order);

	// read flat file
	flat_open(flat, path);
	usage_update(self->arg->memory, storage_size(&flat->storage));

	auto total = (double)storage_size(&flat->storage) / 1024 / 1024;
	info("recover: {u64}/{u64}.{d} ({.2f} MiB)",
	     checkpoint, self->config->id, flat->column->order,
	     total);
}

void
part_open(Part* self, uint64_t checkpoint)
{
	// heap
	part_open_heap(self, checkpoint);

	// vector stores (per column)
	auto primary = part_primary(self);
	auto columns = index_keys(primary)->columns;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->size_flat)
			continue;
		auto flat = flats_at(&self->flats, column);
		part_open_flat(self, flat, checkpoint);
	}
}

void
part_truncate(Part* self)
{
	IndexOp op;
	index_op_init(&op);
	for (auto index = self->indexes; index; index = index->next)
		index_truncate(index, &op);

	// create a new empty heap with matching metadata
	auto new = heap_allocate();
	int64_t delta = -storage_size(&self->heap->storage) + storage_size(&new->storage);
	heap_free(self->heap);
	self->heap = new;

	usage_update(self->arg->memory, op.delta + delta);
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

	// check memory limit
	if (config->type == INDEX_HASH)
	{
		auto size = index_hash_size(config);
		usage_add_reserve(self->arg->memory, size);

		IndexOp op;
		index_op_init(&op);
		index_create(index, &op);
		usage_add(self->arg->memory, size);
	}
}

void
part_index_drop(Part* self, Str* name)
{
	auto index = self->indexes;
	auto tail  = (Index*)NULL;
	for (; index; index = index->next)
	{
		if (str_compare(&index->config->name, name))
			break;
		tail = index;
	}
	if (! index)
		error("index '{str}': not exists", name);

	// unlink index
	if (tail)
		tail->next = index->next;
	else
		self->indexes = index->next;
	self->indexes_count--;

	IndexOp op;
	index_op_init(&op);
	index_free(index, &op);
	usage_update(self->arg->memory, op.delta);
}

Index*
part_index_find(Part* self, Str* name, bool error_if_not_exists)
{
	for (auto index = self->indexes; index; index = index->next)
	{
		if (str_compare(&index->config->name, name))
			return index;
	}
	if (error_if_not_exists)
		error("index '{str}': not exists", name);
	return NULL;
}

void
part_status(Part* self, Buf* buf, int flags)
{
	unused(flags);
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_int(buf, self->config->id);

	// min
	encode_raw(buf, "min", 3);
	encode_int(buf, self->config->hash_min);

	// max
	encode_raw(buf, "max", 3);
	encode_int(buf, self->config->hash_max);

	// size
	encode_raw(buf, "size", 4);
	encode_int(buf, storage_size(&self->heap->storage));

	encode_obj_end(buf);
}
