
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
flush_begin(Flush* self, Table* table, uint64_t id)
{
	self->table = table;

	// create shadow heap
	auto heap_shadow = heap_allocate(false);

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// find partition by id
	auto origin_id = part_mgr_find(&table->part_mgr, id);
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

	// set shadow heap hash min/max
	heap_shadow->header->hash_min = origin->heap->header->hash_min;
	heap_shadow->header->hash_max = origin->heap->header->hash_max;

	// set id and volumes
	auto volumes = &table->part_mgr.config->volumes;
	id_prepare(&self->id_ram, ID_RAM, volumes);

	auto tier = tier_mgr_first(&table->tier_mgr);
	volumes = &tier->config->volumes;
	id_prepare(&self->id_pending, ID_PENDING, volumes);

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
flush_job(intptr_t* argv)
{
	auto self = (Flush*)argv[0];
	auto heap = self->origin->heap;

	// create <id>.ram.incomplete file
	auto heap_temp = heap_allocate(false);
	defer(heap_free, heap_temp);
	heap_temp->header->hash_min = heap->header->hash_min;
	heap_temp->header->hash_max = heap->header->hash_max;
	heap_temp->header->lsn      = self->origin_lsn;
	auto id = &self->id_ram;
	heap_create(heap_temp, &self->file_ram, id, ID_RAM_INCOMPLETE);

	auto total = (double)self->file_ram.size / 1024 / 1024;
	info("flush: %s/%05" PRIu64 ".ram (%.2f MiB)",
	     id->volume->storage->config->name.pos,
	     id->id,
	     total);

	// create <id>.pending.incomplete file
	id_create(&self->id_pending, &self->file_pending, ID_PENDING_INCOMPLETE);	

	// create heap index
	auto keys = index_keys(part_primary(self->origin));
	heap_index(heap, keys, &self->heap_index);

	// prepare heap index iterator
	HeapIndexIterator it;
	heap_index_iterator_init(&it);
	heap_index_iterator_open(&it, &self->heap_index);

	// write pending object
	id = &self->id_pending;
	auto tier = tier_mgr_first(&self->table->tier_mgr);

	auto writer = self->writer;
	writer_reset(writer);
	writer_start(writer, &self->file_pending, id->volume->storage,
	             tier->config->region_size);
	while (heap_index_iterator_has(&it))
	{
		auto row = heap_index_iterator_at(&it);
		writer_add(writer, row);
		heap_index_iterator_next(&it);
	}
	writer_stop(writer);

	total = (double)self->file_pending.size / 1024 / 1024;
	info("flush: %s/%s/%05" PRIu64 ".pending (%.2f MiB)",
	     id->volume->storage->config->name.pos,
	     tier->config->name.pos,
	     id->id,
	     total);

	// create and open object
	self->object = object_allocate(&self->id_pending);
	object_open(self->object, ID_PENDING_INCOMPLETE, true);
}

static void
flush_complete_job(intptr_t* argv)
{
	auto self = (Flush*)argv[0];

	// free shadow heap (former main heap)
	auto origin = self->origin;
	auto shadow = origin->heap_shadow;
	origin->heap_shadow = NULL;
	heap_free(shadow);

	// free indexes
	auto index = self->indexes;
	while (index)
	{
		auto next = index->next;
		index_free(index);
		index = next;
	}
	self->indexes = NULL;

	// create <id>.service.incomplete file
	auto service = self->service_file;
	service_file_begin(service);
	service_file_add_input(service,  &self->id_origin);
	service_file_add_output(service, &self->id_ram);
	service_file_add_output(service, &self->id_pending);
	service_file_end(service);
	service_file_create(service);

	// heap

	// sync incomplete heap file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->file_ram);

	file_close(&self->file_ram);

	// unlink origin heap file
	id_delete(&self->id_origin, ID_RAM);

	// rename
	id_rename(&self->id_ram, ID_RAM_INCOMPLETE, ID_RAM);

	// pending

	// sync incomplete pending file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->file_pending);

	file_close(&self->file_pending);

	// rename
	id_rename(&self->id_pending, ID_PENDING_INCOMPLETE, ID_PENDING);

	// remove service file (complete)
	service_file_delete(service);
}

static void
flush_apply(Flush* self)
{
	auto table  = self->table;
	auto origin = self->origin;

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);
	assert(! origin->track.prepared.list_count);

	// snapshot complete
	heap_snapshot(origin->heap, NULL, false);

	// create a new set of indexes
	self->indexes         = origin->indexes;
	origin->indexes       = NULL;
	origin->indexes_count = 0;
	auto index = self->indexes;
	while (index)
	{
		part_index_create(origin, index->config);
		index = index->next;
	}

	// use shadow heap as main
	auto heap = origin->heap;
	origin->heap = origin->heap_shadow;
	origin->heap_shadow = heap;

	// recreate indexes
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, origin->heap, NULL);
	for (; heap_iterator_has(&it); heap_iterator_next(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		if (chunk->is_shadow_free)
			continue;
		chunk->is_shadow = false;
		auto row = heap_iterator_at(&it);
		auto prev = part_apply(origin, row, false);
		assert(! prev);
	}

	// update volume refs
	volume_unref(origin->id.volume);
	volume_ref(self->id_ram.volume);

	// add object to the tier
	auto tier = tier_mgr_first(&table->tier_mgr);
	tier_add(tier, &self->object->id);
	self->object = NULL;

	// update partition id
	id_copy(&origin->id, &self->id_ram);
}

void
flush_init(Flush* self, Service* service)
{
	ops_lock_init(&self->lock);
	self->origin       = NULL;
	self->origin_lsn   = 0;
	self->object       = NULL;
	self->indexes      = NULL;
	self->writer       = writer_allocate();
	self->table        = NULL;
	self->service_file = service_file_allocate();
	self->service      = service;
	id_init(&self->id_origin);
	id_init(&self->id_ram);
	id_init(&self->id_pending);
	file_init(&self->file_ram);
	file_init(&self->file_pending);
	buf_reset(&self->heap_index);
}

void
flush_free(Flush* self)
{
	service_file_free(self->service_file);
	writer_free(self->writer);
	buf_free(&self->heap_index);
}

void
flush_reset(Flush* self)
{
	ops_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->object     = NULL;
	self->indexes    = NULL;
	self->table      = NULL;
	service_file_reset(self->service_file);
	writer_reset(self->writer);
	id_init(&self->id_origin);
	id_init(&self->id_ram);
	id_init(&self->id_pending);
	file_close(&self->file_ram);
	file_close(&self->file_pending);
	buf_init(&self->heap_index);
}

bool
flush_run(Flush* self, Table* table, uint64_t id)
{
	flush_reset(self);

	// get catalog shared lock and partition service lock
	ops_lock(&self->service->ops, &self->lock, id);
	defer(ops_unlock, &self->lock);

	// find and rotate partition
	if (! flush_begin(self, table, id))
		return false;

	// create heap snapshot
	auto on_error = error_catch
	(
		// run heap snapshot in background
		run(flush_job, 1, self);

		// apply
		flush_apply(self);

		// finilize and cleanup
		run(flush_complete_job, 1, self);
	);

	if (on_error)
		rethrow();

	return true;
}
