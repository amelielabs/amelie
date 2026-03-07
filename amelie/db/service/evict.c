
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
evict_begin(Evict* self, Table* table, uint64_t id)
{
	self->table = table;

	// create shadow heap
	auto heap_shadow = heap_allocate(false);

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// find partition by id
	auto origin = part_mgr_find(&table->part_mgr, id);
	if (! origin)
	{
		heap_free(heap_shadow);
		return false;
	}
	self->origin    = origin;
	self->origin_id = origin->id;

	// set new partition id
	auto volumes = &table->part_mgr.config->volumes;
	auto part_id = &self->part_id;
	part_id->id      = state_psn_next();
	part_id->version = 0;
	part_id->volume  = volume_mgr_next(volumes);

	// commit pending prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	assert(! origin->track.prepared.list_count);
	self->origin_lsn = origin->track.lsn;

	// switch partition shadow heap and begin heap snapshot
	assert(! origin->heap_shadow);
	origin->heap_shadow = heap_shadow;
	heap_snapshot(origin->heap, heap_shadow, true);

	// set shadow heap hash min/max
	heap_shadow->header->hash_min = origin->heap->header->hash_min;
	heap_shadow->header->hash_max = origin->heap->header->hash_max;
	return true;
}

static void
evict_map(Evict* self, EvictBranch* branch, Row* key)
{
	// find object matching the key
	auto table = self->table;
	auto tier  = tier_mgr_first(&table->tier_mgr);
	for (;;)
	{
		// map object matching the key
		auto lock_table = lock(&table->rel, LOCK_SHARED);
		branch->parent_last = false;
		branch->parent = mapping_map(&tier->mapping, key);
		auto parent_id = branch->parent->id.id;
		unlock(lock_table);

		// get the object shared lock
		service_lock(self->service, &branch->lock, LOCK_SHARED, parent_id);

		// ensure the object is still exists
		lock_table = lock(&table->rel, LOCK_SHARED);
		auto match = tier_find(tier, parent_id);
		branch->parent_last = !mapping_next(&tier->mapping, branch->parent);
		unlock(lock_table);

		// shared object lock is held till evict completion
		if (match)
			break;

		// object is no longer valid, retry
		service_unlock(&branch->lock);
		branch->parent = NULL;
	}
}

static EvictBranch*
evict_start(Evict* self, Row* key)
{
	// create new branch
	auto branch = (EvictBranch*)buf_emplace(&self->branches, sizeof(EvictBranch));
	branch->parent      = NULL;
	branch->parent_last = false;
	branch->branch      = NULL;
	service_lock_init(&branch->lock);
	self->branches_count++;

	// find parent object
	evict_map(self, branch, key);

	// create new branch
	branch->branch = branch_allocate(&branch->parent->id, &branch->parent->file);

	// start writer
	auto tier = tier_mgr_first(&self->table->tier_mgr);
	auto writer = self->writer;
	writer_reset(writer);
	writer_start(writer,
	             &branch->parent->file,
	              branch->parent->id.volume->storage,
	             tier->config->region_size);

	return branch;
}

static void
evict_stop(Evict* self, EvictBranch* branch)
{
	auto tier = tier_mgr_first(&self->table->tier_mgr);
	writer_stop(self->writer);

	// set branch meta data
	meta_writer_copy(&self->writer->meta_writer,
	                 &branch->branch->meta,
	                 &branch->branch->meta_data);

	auto id    = &branch->parent->id;
	auto total = (double)branch->parent->file.size / 1024 / 1024;
	info("evict: %s/%s/%05" PRIu64 ".%02" PRIu64 " (%.2f MiB)",
	     id->volume->storage->config->name.pos,
	     tier->config->name.pos,
	     id->id,
	     id->version,
	     total);
}

static void
evict_partition(Evict* self)
{
	auto part_id   = &self->part_id;
	auto part_file = &self->part_file;
	auto heap      =  self->origin->heap;

	// create <id>.incomplete file
	auto heap_temp = heap_allocate(false);
	defer(heap_free, heap_temp);
	heap_temp->header->hash_min = heap->header->hash_min;
	heap_temp->header->hash_max = heap->header->hash_max;
	heap_temp->header->lsn      = self->origin_lsn;
	heap_create(heap_temp, part_file, part_id, STATE_INCOMPLETE);

	auto total = (double)part_file->size / 1024 / 1024;
	info("evict: %s/%05" PRIu64 " (%.2f MiB)",
	     part_id->volume->storage->config->name.pos,
	     part_id->id,
	     total);
}

static void
evict_job(intptr_t* argv)
{
	auto self = (Evict*)argv[0];

	// create <id>.incomplete file
	evict_partition(self);

	// create heap index
	auto keys = index_keys(part_primary(self->origin));
	auto heap = self->origin->heap;
	heap_index(heap, keys, &self->origin_heap_index);

	// prepare heap index iterator
	HeapIndexIterator it;
	heap_index_iterator_init(&it);
	heap_index_iterator_open(&it, &self->origin_heap_index);

	// create branches for every matching objects
	EvictBranch* branch = NULL;
	for (;;)
	{
		if (! heap_index_iterator_has(&it))
		{
			if (branch)
				evict_stop(self, branch);
			break;
		}

		auto row = heap_index_iterator_at(&it);
		if (branch)
		{
			// object is last or row <= branch max
			if (branch->parent_last || branch_in(branch->parent->root, keys, row))
			{
				writer_add(self->writer, row);
				heap_index_iterator_next(&it);
				continue;
			}
		}

		// close writer (finish previous object)
		if (branch)
			evict_stop(self, branch);

		// create new branch and find the parent object
		branch = evict_start(self, row);
	}
}

static void
evict_gc(Evict* self)
{
	// free shadow heap (former main heap)
	auto origin = self->origin;
	auto shadow = origin->heap_shadow;
	origin->heap_shadow = NULL;
	heap_free(shadow);

	// free indexes
	auto index = self->origin_indexes;
	while (index)
	{
		auto next = index->next;
		index_free(index);
		index = next;
	}
	self->origin_indexes = NULL;
}

static void
evict_complete_job(intptr_t* argv)
{
	auto self = (Evict*)argv[0];

	// free shadow heap and indexes
	evict_gc(self);

	// create <id>.service.incomplete file
	auto service = self->service_file;
	service_file_begin(service);
	service_file_add_origin(service, &self->origin_id);
	service_file_add_create(service, &self->part_id);
	auto it  = (EvictBranch*)self->branches.start;
	auto end = (EvictBranch*)self->branches.position;
	for (; it < end; it++)
	{
		// (parent object version is already updated)
		auto id = &it->parent->id;
		service_file_add_rename_version(service, id->id, id->volume,
		                                id->version - 1,
		                                id->version);
	}
	service_file_end(service);
	service_file_create(service);

	// complete partition file

	// sync incomplete partition file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&self->part_file);

	file_close(&self->part_file);

	// unlink origin partition file
	id_delete(&self->origin_id, STATE_COMPLETE);

	// rename
	id_rename(&self->part_id, STATE_INCOMPLETE, STATE_COMPLETE);

	// rename parent object file
	it = (EvictBranch*)self->branches.start;
	for (; it < end; it++)
	{
		// sync object file
		if (opt_int_of(&config()->storage_sync))
			file_sync(&it->parent->file);

		// (parent object version is already updated)
		auto id = &it->parent->id;
		id_rename_version(id->id, id->volume,
		                  id->version - 1,
		                  id->version);
	}

	// remove service file (done)
	service_file_delete(service);
}

static void
evict_apply(Evict* self)
{
	auto origin = self->origin;
	auto table  = self->table;
	auto tier   = tier_mgr_first(&table->tier_mgr);

	// take table exclusive lock (unlock on return)
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);
	assert(! origin->track.prepared.list_count);

	// snapshot complete
	heap_snapshot(origin->heap, NULL, false);

	// create a new set of indexes
	self->origin_indexes  = origin->indexes;
	origin->indexes       = NULL;
	origin->indexes_count = 0;

	auto index = self->origin_indexes;
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
	volume_remove(origin->id.volume, &origin->link_volume);

	// update partition id and volume link
	origin->id = self->part_id;
	volume_add(origin->id.volume, &origin->link_volume);

	// attach branches to the corresponding objects
	auto service = false;
	auto at  = (EvictBranch*)self->branches.start;
	auto end = (EvictBranch*)self->branches.position;
	for (; at < end; at++)
	{
		object_add(at->parent, at->branch);

		// update object version
		at->parent->id.version++;

		// schedule additional service for the table
		if (at->parent->branches_count > tier->config->branches)
			service = true;
	}

	if (service)
		service_add(self->service, &table->config->id);
}

void
evict_init(Evict* self, Service* service)
{
	self->origin         = NULL;
	self->origin_lsn     = 0;
	self->origin_indexes = NULL;
	self->branches_count = 0;
	self->writer         = writer_allocate();
	self->table          = NULL;
	self->service_file   = service_file_allocate();
	self->service        = service;

	service_lock_init(&self->lock);
	buf_init(&self->origin_heap_index);
	id_init(&self->origin_id);
	id_init(&self->part_id);
	file_init(&self->part_file);
	buf_init(&self->branches);
}

void
evict_free(Evict* self)
{
	evict_reset(self);

	buf_free(&self->origin_heap_index);
	buf_free(&self->branches);
	writer_free(self->writer);
	service_file_free(self->service_file);
}

void
evict_reset(Evict* self)
{
	file_close(&self->part_file);

	self->origin         = NULL;
	self->origin_lsn     = 0;
	self->origin_indexes = NULL;
	self->branches_count = 0;
	self->table          = NULL;

	service_file_reset(self->service_file);
	service_lock_init(&self->lock);
	writer_reset(self->writer);
	buf_reset(&self->origin_heap_index);
	id_init(&self->origin_id);
	id_init(&self->part_id);
	file_init(&self->part_file);
	buf_reset(&self->branches);
}

bool
evict_run(Evict* self, Table* table, uint64_t id)
{
	assert(tier_mgr_created(&table->tier_mgr));
	evict_reset(self);

	// lock partition by id
	service_lock(self->service, &self->lock, LOCK_EXCLUSIVE, id);
	defer(service_unlock, &self->lock);

	// find and rotate partition
	if (! evict_begin(self, table, id))
		return false;

	auto on_error = error_catch
	(
		// run evict in background
		run(evict_job, 1, self);

		// apply
		evict_apply(self);

		// finilize and cleanup
		run(evict_complete_job, 1, self);
	);

	// unlock parent objects
	auto it  = (EvictBranch*)self->branches.start;
	auto end = (EvictBranch*)self->branches.position;
	for (; it < end; it++)
		service_unlock(&it->lock);

	// todo: abort

	if (on_error)
		rethrow();

	return true;
}
