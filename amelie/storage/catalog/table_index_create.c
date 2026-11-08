
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
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static inline bool
db_indexate_with_null(Index* index, Row* row)
{
	for (auto at = 0; at < index->config->keys.count; at++)
	{
		auto key = keys_at(&index->config->keys, at);
		if (! row_column(row, key->column))
			return true;
	}
	return false;
}

hot static void
db_indexate(Part* part, IndexConfig* config)
{
	// allocate index
	Index* index;	
	if (config->type == INDEX_TREE)
		index = index_tree_allocate(config, part);
	else
	if (config->type == INDEX_HASH)
		index = index_hash_allocate(config, part);
	else
		abort();
	assert(! part->in_progress);
	part->in_progress = index;

	auto it_upsert = index_iterator(index);
	defer(iterator_close, it_upsert);

	// ensure memory limit and allocate hash
	auto memory = part->arg->memory;
	if (config->type == INDEX_HASH)
	{
		auto size = index_hash_size(config);
		usage_add_reserve(memory, size);

		IndexOp op;
		index_op_init(&op);
		index_create(index, &op);
		usage_add(memory, op.delta);
	}

	IndexOp io =
	{
		.row      = NULL,
		.row_prev = NULL,
		.it       = it_upsert,
		.delta    = 0
	};

	// indexate heap
	auto heap = part->heap;
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap, false);
	for (;; heap_iterator_next(&it))
	{
		auto row = heap_iterator_at(&it);
		if (! row)
			break;

		if (unlikely(db_indexate_with_null(index, row)))
			error("create index: null key column");

		// get index head
		io.row   = row;
		io.delta = 0;
		auto exists = index_upsert(index, &io);
		if (exists)
			error("create index: index unique constraint violation");

		// ensure memory limit
		usage_add(memory, io.delta);
	}

	auto total = (double)pages_size(&heap->pages) / 1024 / 1024;
	info("create index: ({.2f} MiB)", total);
}

bool
table_index_create(Catalog*     self,
                   Tr*          tr,
                   Table*       table,
                   IndexConfig* config,
                   bool         if_not_exists)
{
	unused(self);

	// check for retention
	if (table->config->retention >= 0)
		error("table '{str}': indexes cannot be used with retention",
		       &table->config->name);

	// find index
	auto index = table_index_find(table, &config->name, false);
	if (index)
	{
		if (! if_not_exists)
			error("table '{str}' index '{str}': already exists",
			      &table->config->name,
			      &config->name);

		index_config_free(config);
		return false;
	}

	// check limit
	if (unlikely(! limits_check(tr->local->limits, LIMIT_INDEXES, table->config->indexes_count + 1)))
		error("table '{str}': indexes limit reached",
		      &table->config->name);

	auto part = table->part;
	auto on_error = error_catch
	(
		db_indexate(part, config);
	);

	auto db = db_of(tr->db);
	if (on_error)
	{
		// abort index creation
		IndexOp io;
		index_op_init(&io);
		if (part->in_progress)
		{
			index_free(part->in_progress, &io);
			part->in_progress = NULL;
		}
		usage_update(&db->memory, io.delta);
		rethrow();
	}

	// attach index
	assert(part->in_progress);
	part_index_add(part, part->in_progress);
	part->in_progress = NULL;

	// attach index to the table
	table_index_add(&db->catalog, table, tr, config);
	return true;
}
