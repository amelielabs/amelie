
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
#include <amelie_object.h>
#include <amelie_tier.h>
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

	// find storage
	auto volumes = &part_mgr->config->volumes;
	Volume* volume = NULL;
	if (storage)
	{
		volume = volume_mgr_find(volumes, storage);
		if (! volume)
			return false;
	}

	// create shadow heap
	auto heap_shadow = heap_allocate(false);

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// find partition by id
	auto origin_id = part_mgr_find(part_mgr, id);
	if (! origin_id)
	{
		heap_free(heap_shadow);
		return false;
	}
	if (origin_id->type != ID_RAM)
	{
		heap_free(heap_shadow);
		return false;
	}
	auto origin  = part_of(origin_id);
	self->origin = origin;
	id_copy(&self->id_origin, &origin->id);

	// set id and volumes
	if (volume)
		id_prepare_in(&self->id_ram, ID_RAM, volume);
	else
		id_prepare(&self->id_ram, ID_RAM, volumes);

	// commit pending prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	assert(! origin->track.prepared.list_count);
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
	auto id = &self->id_ram;
	heap->header->lsn = self->origin_lsn;
	heap_create(heap, &self->file_ram, id, ID_RAM_INCOMPLETE);

	auto total = (double)self->file_ram.size / 1024 / 1024;
	info("checkpoint: %s/%05" PRIu64 ".ram (%.2f MiB)",
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

	// create <id>.service.incoplete file
	auto service = self->service_file;
	service_file_begin(service);
	service_file_add_input(service,  &self->id_origin);
	service_file_add_output(service, &self->id_ram);
	service_file_end(service);
	service_file_create(service);

	// sync incomplete heap file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->file_ram);

	file_close(&self->file_ram);

	// unlink origin heap file
	id_delete(&self->id_origin, ID_RAM);

	// rename
	id_rename(&self->id_ram, ID_RAM_INCOMPLETE, ID_RAM);

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
		auto prev = part_apply(origin, row, false);
		assert(prev == row_shadow);
	}

	// update volume refs
	volume_unref(origin->id.volume);
	volume_ref(self->id_ram.volume);

	// update partition id
	id_copy(&origin->id, &self->id_ram);
}

void
refresh_init(Refresh* self, Service* service)
{
	ops_lock_init(&self->lock);
	self->origin       = NULL;
	self->origin_lsn   = 0;
	self->service_file = service_file_allocate();
	self->service      = service;
	self->table        = NULL;
	id_init(&self->id_origin);
	id_init(&self->id_ram);
	file_init(&self->file_ram);
}

void
refresh_free(Refresh* self)
{
	service_file_free(self->service_file);
}

void
refresh_reset(Refresh* self)
{
	ops_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->table      = NULL;
	service_file_reset(self->service_file);
	id_init(&self->id_origin);
	id_init(&self->id_ram);
	file_close(&self->file_ram);
}

bool
refresh_run(Refresh* self, Table* table, uint64_t id, Str* storage)
{
	refresh_reset(self);

	// get catalog shared lock and partition service lock
	ops_lock(&self->service->ops, &self->lock, id);
	defer(ops_unlock, &self->lock);

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
