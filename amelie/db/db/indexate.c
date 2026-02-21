
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
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

static bool
indexate_match(Indexate* self, IndexConfig* config)
{
	// find next partition which does not yet have index
	auto table = self->table;
	for (;;)
	{
		// take table shared lock
		auto lock_table = lock(&table->rel, LOCK_SHARED);

		uint64_t id = UINT64_MAX;
		list_foreach(&engine_main(&table->engine)->list_ram)
		{
			auto part = list_at(Part, id.link);
			auto index = part_index_find(part, &config->name, false);
			if (index)
				continue;
			id = part->id.id;
		}

		unlock(lock_table);

		// nothing left to do
		if (id == UINT64_MAX)
			break;

		// get partition service lock
		ops_lock(&self->db->ops, &self->lock, id);

		// find partition by id
		auto origin_id = engine_find(&table->engine, id);
		if (origin_id && origin_id->type == ID_RAM)
		{
			self->origin = part_of(origin_id);
			return true;
		}

		// partition id no longer available, retry
		ops_unlock(&self->lock);
	}

	return false;
}

static bool
indexate_begin(Indexate* self, Table* table, IndexConfig* config)
{
	self->table = table;

	// find next partition which does not yet have index
	if  (! indexate_match(self, config))
		return false;

	// allocate index
	if (config->type == INDEX_TREE)
		self->index = index_tree_allocate(config, self->origin);
	else
	if (config->type == INDEX_HASH)
		self->index = index_hash_allocate(config, self->origin);
	else
		abort();

	// create shadow heap
	auto heap_shadow = heap_allocate(false);

	// take table exclusive lock
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);

	// commit pending prepared transactions
	auto origin = self->origin;
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);

	// switch partition shadow heap and begin heap snapshot
	assert(! origin->heap_shadow);
	origin->heap_shadow = heap_shadow;
	heap_snapshot(origin->heap, heap_shadow, true);

	unlock(lock_table);
	return true;
}

static void
indexate_job(intptr_t* argv)
{
	auto self   = (Indexate*)argv[0];
	auto origin = self->origin;
	auto heap   = origin->heap;

	// indexate heap
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap, NULL);
	for (; heap_iterator_has(&it); heap_iterator_next(&it))
	{
		auto row  = heap_iterator_at(&it);
		auto prev = index_replace_by(self->index, row);
		if (unlikely(prev))
			error("indexate: index unique constraint violation");
	}

	auto total = (double)page_mgr_used(&heap->page_mgr) / 1024 / 1024;
	auto id = &origin->id;
	info("indexate: %s/%s/%05" PRIu64 ".ram (%.2f MiB)",
	     id->storage->storage->config->name.pos,
	     id->tier->name.pos,
	     id->id,
	     total);
}

static void
indexate_complete_job(intptr_t* argv)
{
	auto self = (Indexate*)argv[0];

	// free shadow heap
	auto origin = self->origin;
	auto shadow = origin->heap_shadow;
	origin->heap_shadow = NULL;
	heap_free(shadow);
}

static void
indexate_apply(Indexate* self)
{
	auto table  = self->table;
	auto origin = self->origin;

	// take table exclusive lock
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// snapshot complete
	heap_snapshot(origin->heap, NULL, false);

	// apply updates during heap snapshot
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, origin->heap_shadow, NULL);
	for (; heap_iterator_has(&it); heap_iterator_next(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		if (chunk->is_shadow_free)
		{
			// delayed heap removal
			heap_remove(origin->heap, *(void**)chunk->data);
			continue;
		}

		// copy row
		auto row_shadow = heap_iterator_at(&it);
		auto row = row_copy(origin->heap, row_shadow);

		// update existing indexes using row copy (replace shadow copy)
		part_apply(origin, row, false);

		// update new index
		auto prev = index_replace_by(self->index, row);
		if (unlikely(prev))
			error("indexate: index unique constraint violation");
	}

	// attach index to the partition
	part_index_add(self->origin, self->index);
	self->index = NULL;
}

static void
indexate_abort(Indexate* self, IndexConfig* config)
{
	// take table exclusive lock
	auto table = self->table;
	auto table_lock = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, table_lock);

	list_foreach(&engine_main(&table->engine)->list_ram)
	{
		auto part  = list_at(Part, id.link);
		auto index = part_index_find(part, &config->name, false);
		if (! index)
			continue;
		// todo: free indexes out of the lock
		part_index_drop(part, &config->name);
	}
}

void
indexate_init(Indexate* self, Db* db)
{
	ops_lock_init(&self->lock);
	self->origin = NULL;
	self->table  = NULL;
	self->index  = NULL;
	self->db     = db;
}

void
indexate_reset(Indexate* self)
{
	ops_lock_init(&self->lock);
	self->origin = NULL;
	self->table  = NULL;
	if (self->index)
	{
		index_free(self->index);
		self->index = NULL;
	}
}

bool
indexate_next(Indexate* self, Table* table, IndexConfig* config)
{
	indexate_reset(self);

	// find next partition
	if (! indexate_begin(self, table, config))
		return false;

	// create heap index
	auto on_error = error_catch
	(
		// create heap index in background
		run(indexate_job, 1, self);

		// apply
		indexate_apply(self);

		// finilize and cleanup
		run(indexate_complete_job, 1, self);
	);

	// unlock
	ops_unlock(&self->lock);

	if (on_error)
	{
		// drop all created indexes from the table partitions
		indexate_abort(self, config);
		if (self->index)
		{
			index_free(self->index);
			self->index = NULL;
		}

		rethrow();
	}

	return true;
}
