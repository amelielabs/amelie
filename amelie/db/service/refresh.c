
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
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>

static bool
refresh_begin(Refresh* self, Table* table, uint64_t id, Str* storage)
{
	auto part_mgr = &table->part_mgr;
	self->table = table;

	// create shadow heap
	auto heap_shadow = heap_allocate();

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// find storage
	auto volumes = &part_mgr->config->volumes;
	Volume* volume = NULL;
	if (storage)
	{
		volume = volume_mgr_find(volumes, storage);
		if (! volume)
		{
			heap_free(heap_shadow);
			return false;
		}
	} else {
		volume = volume_mgr_next(volumes);
	}

	// set new partition id
	auto part_id = &self->part_id;
	part_id->id     = state_psn_next();
	part_id->volume = volume;

	// find partition by id
	auto origin = part_mgr_find(part_mgr, id);
	if (! origin)
	{
		heap_free(heap_shadow);
		return false;
	}
	self->origin    = origin;
	self->origin_id = origin->id;

	// commit pending prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	assert(! origin->track.prepared.list_count);
	self->origin_lsn = origin->track.lsn;
	self->origin_tsn = origin->track.consensus.commit;

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

	// create <id>.incomplete file
	auto id = &self->part_id;
	heap->header->lsn = self->origin_lsn;
	heap->header->tsn = self->origin_tsn;
	heap_create(heap, &self->part_file, id, ID_PARTITION_INCOMPLETE);

	auto total = (double)self->part_file.size / 1024 / 1024;
	info("refresh: %s/%05" PRIu64 ".partition (%.2f MiB)",
	     id->volume->storage->config->name.pos,
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

	// create <id>.service.incomplete file
	auto service = self->service_file;
	service_file_begin(service);
	service_file_add_origin(service, &self->origin_id, ID_PARTITION);
	service_file_add_create(service, &self->part_id, ID_PARTITION);
	service_file_end(service);
	service_file_create(service);

	// sync incomplete heap file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->part_file);

	file_close(&self->part_file);

	// unlink origin heap file
	id_delete(&self->origin_id, ID_PARTITION);

	// rename
	id_rename(&self->part_id, ID_PARTITION_INCOMPLETE, ID_PARTITION);

	// remove service file (complete)
	service_file_delete(service);
}

static void
refresh_apply(Refresh* self)
{
	auto table  = self->table;
	auto origin = self->origin;

	// case 3: access before apply
	breakpoint(REL_BP_REFRESH_3);

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// case 4: access during apply
	breakpoint(REL_BP_REFRESH_4);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);
	assert(! origin->track.prepared.list_count);

	// snapshot complete
	heap_snapshot(origin->heap, NULL, false);

	// apply updates during heap snapshot
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, origin->heap_shadow, NULL);
	for (; iterator_has(&it.it); heap_iterator_next(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		if (chunk->is_shadow_free)
		{
			// delayed heap removal
			row_free(origin->heap, *(Row**)chunk->data);
			continue;
		}

		// copy row
		auto row_shadow = it.it.current;
		auto row = row_copy(origin->heap, row_shadow);

		// update indexes using row copy (replace shadow copy)
		auto prev = part_apply(origin, row, false);
		unused(prev);
		assert(prev == row_shadow);
	}

	// update volume refs
	volume_remove(origin->id.volume, &origin->link_volume);

	// update partition id and volume link
	origin->id = self->part_id;
	volume_add(origin->id.volume, &origin->link_volume);
}

void
refresh_init(Refresh* self, Service* service)
{
	service_lock_init(&self->lock);
	self->origin       = NULL;
	self->origin_lsn   = 0;
	self->origin_tsn   = 0;
	self->service_file = service_file_allocate();
	self->service      = service;
	self->table        = NULL;
	id_init(&self->origin_id);
	id_init(&self->part_id);
	file_init(&self->part_file);
}

void
refresh_free(Refresh* self)
{
	service_file_free(self->service_file);
}

void
refresh_reset(Refresh* self)
{
	service_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->origin_tsn = 0;
	self->table      = NULL;
	service_file_reset(self->service_file);
	id_init(&self->origin_id);
	id_init(&self->part_id);
	file_close(&self->part_file);
}

bool
refresh_run(Refresh* self, Table* table, uint64_t id, Str* storage)
{
	refresh_reset(self);

	// lock partition by id
	service_lock(self->service, &self->lock, LOCK_EXCLUSIVE, id);
	defer(service_unlock, &self->lock);

	// case 1: concurrent partition refresh
	breakpoint(REL_BP_REFRESH_1);

	// find and rotate partition
	if (! refresh_begin(self, table, id, storage))
		return false;

	// case 2: update during refresh
	breakpoint(REL_BP_REFRESH_2);

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

	if (on_error)
		rethrow();

	return true;
}
