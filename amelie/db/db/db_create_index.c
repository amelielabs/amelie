
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
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>
#include <amelie_db.h>

static inline bool
db_indexate_with_null(Index* index, Row* row)
{
	list_foreach(&index->config->keys.list)
	{
		const auto column = list_at(Key, link)->column;
		if (! row_column(row, column))
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

	// indexate heap
	auto heap = part->heap;
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap, NULL);
	for (; heap_iterator_has(&it); heap_iterator_next(&it))
	{
		auto row = heap_iterator_at(&it);
		if (unlikely(db_indexate_with_null(index, row)))
			error("create index: null key column");
		// get index head
		if (! index_upsert(index, row, it_upsert))
			continue;
		// check unique constraint violation
		auto head = iterator_at(it_upsert);
		if (row_unique(head, heap))
			error("create index: index unique constraint violation");
	}

	auto total = (double)storage_size(&heap->storage) / 1024 / 1024;
	info("create index: partition {u64} ({.2f} MiB)",
	     part->config->id, total);
}

void
db_create_index(Db* self, Tr* tr, uint8_t* op, int flags)
{
	// one checkpoint (or create index) at a time
	auto checkpoint_lock = lock_system(REL_CHECKPOINT, LOCK_EXCLUSIVE);
	defer(unlock, checkpoint_lock);

	// take shared catalog lock
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);
	defer(unlock, lock_catalog);

	Str  name_user;
	Str  name;
	auto pos = table_op_index_create_read(op, &name_user, &name);

	// find table
	auto table = catalog_find_table(&self->catalog, &name_user, &name, false);
	if (! table)
		error("table '{str}': not exists", &name);

	// only owner or superuser
	check_ownership(tr, &table->rel);

	auto config = index_config_read(table_columns(table), &pos);
	errdefer(index_config_free, config);
	keys_set_primary(&config->keys, false);

	// find index
	auto if_not_exists = ddl_if_not_exists(flags);
	auto index = table_index_find(table, &config->name, false);
	if (index)
	{
		if (! if_not_exists)
			error("table '{str}' index '{str}': already exists",
			      &table->config->name,
			      &config->name);

		index_config_free(config);
		return;
	}

	// create indexes on every table partition under the exclusive_ro lock,
	// this will make table accessible on backends, while
	// preventing writes
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE_RO);
	auto on_error = error_catch
	(
		list_foreach(&table->parts.list)
			db_indexate(list_at(Part, link), config);
	);
	unlock(lock_table);

	if (on_error)
	{
		// abort index creation
		list_foreach(&table->parts.list)
		{
			auto part = list_at(Part, link);
			if (! part->in_progress)
				continue;
			index_free(part->in_progress);
			part->in_progress = NULL;
		}
		unlock(lock_catalog);
		rethrow();
	}

	unlock(lock_catalog);

	// take exclusive catalog lock (HELD till completion)
	lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);

	// todo: validate table again

	// attach indexes to partitions
	list_foreach(&table->parts.list)
	{
		auto part = list_at(Part, link);
		assert(part->in_progress);
		part_index_add(part, part->in_progress);
		part->in_progress = NULL;
	}

	// attach index to the table
	table_index_add(&self->catalog, table, tr, config);
}
