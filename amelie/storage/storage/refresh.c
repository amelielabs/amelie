
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
#include <amelie_tier>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
refresh_init(Refresh* self, Storage* storage)
{
	service_lock_init(&self->lock);
	self->origin        = NULL;
	self->origin_heap   = NULL;
	self->origin_object = NULL;
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
	service_lock_init(&self->lock);
	self->origin        = NULL;
	self->origin_heap   = NULL;
	self->origin_object = NULL;
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
	self->volume = volume_mgr_find_volume(&table->volume_mgr, tier);
	if (! self->volume)
		return false;

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

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// rotate partition heap
	assert(origin->heap == &origin->heap_a);
	origin->heap      = &origin->heap_b;
	self->origin_heap = &origin->heap_a;

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
	return true;
}

static void
refresh_merge(Refresh* self)
{
	// merge heap with existing partition
	auto merger = &self->merger;
	MergerConfig config =
	{
		.type   = MERGER_SNAPSHOT,
		.origin = self->origin->object,
		.heap   = self->origin_heap,
		.keys   = self->table->volume_mgr.mapping.keys,
		.source = self->volume->tier->config
	};
	merger_execute(merger, &config);
}

static void
refresh_apply_replace(Refresh* self)
{
	auto storage = self->storage;
	auto table   = self->table;
	auto origin  = self->origin;

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

	// todo: redistribute

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
}

static void
refresh_apply(Refresh* self)
{
	if (self->merger.objects_count == 1)
		refresh_apply_replace(self);
}

static void
refresh_end(Refresh* self)
{
	// todo: sync

	// remove origin object
	if (self->origin_object)
	{
		object_delete(self->origin_object, ID);
		object_free(self->origin_object);
		self->origin_object = NULL;
	}
}

void
refresh_run(Refresh* self, Id* id, Str* tier)
{
	// get catalog shared lock and partition service lock
	storage_lock(self->storage, &self->lock, id);

	// find and rotate partition
	refresh_begin(self, id, tier);

	// merge, apply, and cleanup
	auto on_error = error_catch
	(
		refresh_merge(self);
		refresh_apply(self);
		refresh_end(self);
	);

	// unlock
	storage_unlock(self->storage, &self->lock);

	if (on_error)
		rethrow();
}
