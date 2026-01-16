
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_partition.h>
#include <amelie_tier.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
refresh_init(Refresh* self, Storage* storage)
{
	part_lock_init(&self->lock);
	self->origin        = NULL;
	self->origin_object = NULL;
	self->lsn           = 0;
	self->table         = NULL;
	self->volume        = NULL;
	self->storage       = storage;
	merger_init(&self->merger);
}

void
refresh_free(Refresh* self)
{
	merger_free(&self->merger);
}

void
refresh_reset(Refresh* self)
{
	part_lock_init(&self->lock);
	self->origin        = NULL;
	self->origin_object = NULL;
	self->lsn           = 0;
	self->table         = NULL;
	self->volume        = NULL;
	merger_reset(&self->merger);
}

static bool
refresh_begin(Refresh* self, Id* id, Str* tier)
{
	auto storage = self->storage;

	// find table by uuid
	auto table = table_mgr_find_by(&storage->catalog.table_mgr, &id->id_table, false);
	if (! table)
		return false;
	self->table = table;

	// find volume by tier name
	if (tier)
	{
		self->volume = volume_mgr_find_volume(&table->volume_mgr, tier);
		if (! self->volume)
			return false;
	}

	// take table exclusive lock
	lock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// find partition by id
	auto origin = volume_mgr_find(&table->volume_mgr, self->lock.id->id);
	if (! origin)
	{
		unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
		return false;
	}
	self->origin = origin;
	self->origin_object = origin->object;
	if (! self->volume)
		self->volume = origin->volume;

	// force commit prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	self->lsn = origin->track.lsn;

	// switch to heap_b
	assert(origin->heap == &origin->heap_a);
	origin->heap = &origin->heap_b;

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
	return true;
}

static void
refresh_merge_job(intptr_t* argv)
{
	// todo: sort heap

	// merge heap with existing partition
	auto self = (Refresh*)argv[0];
	auto merger = &self->merger;
	MergerConfig config =
	{
		.lsn    = self->lsn,
		.origin = self->origin->object,
		.heap   = &self->origin->heap_a,
		.keys   = self->table->volume_mgr.mapping.keys,
		.source = self->volume->tier->config
	};
	merger_execute(merger, &config);

	if (merger->objects_count > 1)
	{
		// todo: create partitions, create indexes
		// todo: load partitions in memory
	}
}

static void
refresh_merge(Refresh* self)
{
	// run merge in background
	run(refresh_merge_job, 1, self);
}

static void
refresh_apply_origin(Refresh* self)
{
	auto storage = self->storage;
	auto table   = self->table;
	auto origin  = self->origin;
	auto source  = self->volume->tier->config;

	// take table exclusive lock
	lock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// update partition object
	auto object = merger_first(&self->merger);
	origin->id     = object->id;
	origin->source = object->source;
	origin->object = object;

	// update volume
	if (origin->volume != self->volume)
	{
		volume_remove(origin->volume, origin);
		volume_add(self->volume, origin);
	}

	// switch back to heap_a
	assert(origin->heap == &origin->heap_b);
	origin->heap = &origin->heap_a;

	if (source->in_memory)
	{
		// todo: 
			// iterate heap_b
			// 	apply changes to heap_a, apply deletes
			//		keep deletes in heap_a, if not in-memory
			//  update indexes

		// (free heap_b on end)
	} else
	{
		// reuse heap_b as heap_a
		// (free previous heap_a on end)
	}

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// (free/remove origin object on end)
}

static void
refresh_apply_split(Refresh* self)
{
	auto storage = self->storage;
	auto table   = self->table;
	auto origin  = self->origin;

	// take table exclusive lock
	lock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// todo:
		// iterate heap_b
		//  distribute between partitions
		//		keep deletes in heap_a, if not in-memory
		//  update indexes

	// remove old partition
		// send undeploy

	// add all partitions to mapping
		// update volume
		// send deploy

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// wait for undeploy (origin) + deploy all parts

	// (free heap_b on end)
	// (free origin partition and free/remove origin object on end)
}

static void
refresh_end(Refresh* self)
{
	// todo: sync new partitions
	(void)self;

#if 0
	// todo: free heap

	// remove origin object
	if (self->origin_object)
	{
		object_delete(self->origin_object, ID);
		object_free(self->origin_object);
		self->origin_object = NULL;
	}

	// todo: free origin partition, if not update
#endif

	self->merger.objects_count = 0;
	list_init(&self->merger.objects);
}

void
refresh_run(Refresh* self, Id* id, Str* tier)
{
	refresh_reset(self);

	// get catalog shared lock and partition service lock
	storage_lock(self->storage, &self->lock, id);

	// find and rotate partition
	refresh_begin(self, id, tier);

	// merge, apply, and cleanup
	auto on_error = error_catch
	(
		refresh_merge(self);

		// update existing partition object
		if (self->merger.objects_count == 1)
			refresh_apply_origin(self);
		else
			refresh_apply_split(self);

		refresh_end(self);
	);

	// unlock
	storage_unlock(self->storage, &self->lock);

	if (on_error)
		rethrow();
}
