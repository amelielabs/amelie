
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
refresh_begin(Refresh* self, Uuid* id_table, uint64_t id, Str* storage)
{
	auto db = self->db;

	// find table by uuid
	auto table = table_mgr_find_by(&db->catalog.table_mgr, id_table, false);
	if (! table)
		return false;
	self->table = table;

	// create shadow heap
	auto heap_shadow = heap_allocate();

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// find partition by id
	auto origin = engine_find(&table->engine, id);
	if (! origin)
	{
		heap_free(heap_shadow);
		return false;
	}
	self->origin = origin;

	// set id
	self->id_origin = origin->id;
	self->id = origin->id;
	self->id.id = state_psn_next();

	// pick storage to use
	TierStorage* storage_ref;
	if (storage)
	{
		auto ref = tier_storage_find(self->id.tier, storage);
		if (! ref)
		{
			heap_free(heap_shadow);
			return false;
		}
		storage_ref = ref;
	} else
	{
		// choose next available storage to use
		storage_ref = tier_storage_next(self->id.tier);
	}
	self->id.storage = storage_ref;

	// commit pending prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	self->origin_lsn = origin->track.lsn;

	// switch partition shadow heap and begin heap snapshot
	assert(! origin->heap_shadow);
	origin->heap_shadow = heap_shadow;
	heap_snapshot(origin->heap, heap_shadow, true);
	return true;
}

static void
refresh_snapshot_job(intptr_t* argv)
{
	auto self   = (Refresh*)argv[0];
	auto origin = self->origin;
	auto heap   = origin->heap;

	// create <id>.ram.incomplete file
	auto id = &self->id;
	heap->header->lsn = self->origin_lsn;
	heap_create(heap, &self->file, id, ID_RAM_INCOMPLETE);

	auto total = (double)page_mgr_used(&heap->page_mgr) / 1024 / 1024;
	info("checkpoint: %s/%s/%05" PRIu64 ".ram (%.2f MiB)",
	     id->storage->storage->config->name.pos,
	     id->tier->name.pos,
	     id->id,
	     total);

	// create <id>.service.incomplete file
	auto service = self->service;
	service_set_id(service, id);
	service_begin(service);
	service_add_input(service, self->id_origin.id);
	service_add_output(service, self->id.id);
	service_end(service);
	service_create(service, &self->file_service, ID_SERVICE_INCOMPLETE);
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

	// sync incomplete service file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->file_service);

	file_close(&self->file_service);

	// rename
	id_rename(&self->id, ID_SERVICE_INCOMPLETE, ID_SERVICE);

	// sync incomplete heap file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->file);

	file_close(&self->file);

	// unlink origin heap file
	id_delete(&self->id_origin, ID_RAM);

	// rename
	id_rename(&self->id, ID_RAM_INCOMPLETE, ID_RAM);

	// remove service files (complete)
	id_delete(&self->id, ID_SERVICE);
}

static void
refresh_apply(Refresh* self)
{
	auto table  = self->table;
	auto origin = self->origin;

	// take table exclusive lock (unlock on return)
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

		// update indexes using row copy (replace shadow copy)
		part_apply(origin, row, false);
	}

	// update storage refs
	tier_storage_unref(origin->id.storage);
	tier_storage_ref(self->id.storage);

	// update partition id
	origin->id = self->id;
}

void
refresh_init(Refresh* self, Db* db)
{
	part_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->table      = NULL;
	self->service    = service_allocate();
	self->db         = db;
	id_init(&self->id_origin);
	id_init(&self->id);
	file_init(&self->file);
	file_init(&self->file_service);
}

void
refresh_free(Refresh* self)
{
	service_free(self->service);
}

void
refresh_reset(Refresh* self)
{
	part_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->table      = NULL;
	service_reset(self->service);
	id_init(&self->id_origin);
	id_init(&self->id);
	file_close(&self->file);
	file_close(&self->file_service);
	file_init(&self->file);
	file_init(&self->file_service);
}

void
refresh_run(Refresh* self, Uuid* id_table, uint64_t id, Str* storage)
{
	refresh_reset(self);

	// get catalog shared lock and partition service lock
	db_lock(self->db, &self->lock, id);

	// find and rotate partition
	if (! refresh_begin(self, id_table, id, storage))
	{
		db_unlock(self->db, &self->lock);
		error("partition or tier storage not found");
	}

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
