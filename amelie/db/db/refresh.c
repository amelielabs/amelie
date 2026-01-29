
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
#include <amelie_tier.h>
#include <amelie_partition.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

static bool
refresh_begin(Refresh* self, Uuid* id_table, uint64_t id)
{
	auto db = self->db;

	// find table by uuid
	auto table = table_mgr_find_by(&db->catalog.table_mgr, id_table, false);
	if (! table)
		return false;
	self->table = table;

	// create shadow heap
	auto heap_shadow = heap_allocate();

	// take table exclusive lock
	lock(&db->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// find partition by id
	auto origin = part_mgr_find(&table->part_mgr, id);
	if (! origin)
	{
		unlock(&db->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
		heap_free(heap_shadow);
		return false;
	}
	self->origin = origin;

	// commit pending prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	self->origin_lsn = origin->track.lsn;

	// switch partition shadow heap and begin heap snapshot
	assert(! origin->heap_shadow);
	origin->heap_shadow = heap_shadow;
	heap_snapshot(origin->heap, heap_shadow, true);

	unlock(&db->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
	return true;
}

static void
refresh_snapshot_job(intptr_t* argv)
{
	// create <id>.ram.incomplete file
	auto self   = (Refresh*)argv[0];
	auto origin = self->origin;
	auto id     = &origin->id;

	auto heap = origin->heap;
	heap->header->lsn = self->origin_lsn;
	heap_create(heap, &self->file, id, ID_RAM_INCOMPLETE);

	auto total = (double)page_mgr_used(&heap->page_mgr) / 1024 / 1024;
	info("checkpoint: %s/%s/%05" PRIu64 ".ram (%.2f MiB)",
	     id->storage->config->name.pos,
	     id->tier->config->name.pos,
	     id->id,
	     total);
}

static void
refresh_complete_job(intptr_t* argv)
{
	auto self = (Refresh*)argv[0];

	// free shadow heap
	auto origin = self->origin;
	auto shadow = origin->heap_shadow;
	origin->heap_shadow = NULL;
	heap_free(shadow);

	// sync incomplete heap file
	if (origin->id.storage->config->sync)
		file_sync(&self->file);

	// unlink origin heap file
	id_delete(&origin->id, ID_RAM);

	// rename
	id_rename(&origin->id, ID_RAM_INCOMPLETE, ID_RAM);
}

static void
refresh_apply(Refresh* self)
{
	auto lock_mgr = &self->db->lock_mgr;
	auto table    = self->table;
	auto origin   = self->origin;

	// take table exclusive lock
	lock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// snapshot complete
	heap_snapshot(origin->heap, NULL, false);

	// apply updates during heap snapshot
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, origin->heap_shadow, NULL);
	while (heap_iterator_has(&it))
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

		// update indexes using row copy (replace shadow copy)
		part_apply(origin, row, false);
	}

	unlock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);
}

void
refresh_init(Refresh* self, Db* db)
{
	part_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->table      = NULL;
	self->db         = db;
	file_init(&self->file);
}

void
refresh_free(Refresh* self)
{
	unused(self);
}

void
refresh_reset(Refresh* self)
{
	part_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->table      = NULL;
	file_close(&self->file);
	file_init(&self->file);
}

void
refresh_run(Refresh* self, Uuid* id_table, uint64_t id)
{
	refresh_reset(self);

	// get catalog shared lock and partition service lock
	db_lock(self->db, &self->lock, id);

	// find and rotate partition
	refresh_begin(self, id_table, id);

	// create heap snapshot
	auto on_error = error_catch
	(
		// run heap snapshot in background
		run(refresh_snapshot_job, 1, self);

		// apply
		refresh_apply(self);

		// finilize and cleanup
		run(refresh_complete_job, 1, self);
	);

	// unlock
	db_unlock(self->db, &self->lock);

	if (on_error)
		rethrow();
}
