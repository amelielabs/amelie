
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
	self->origin      = NULL;
	self->origin_heap = NULL;
	self->table       = NULL;
	self->parts_count = 0;
	self->volume      = NULL;
	self->storage     = storage;
	list_init(&self->parts);
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
	self->origin      = NULL;
	self->origin_heap = NULL;
	self->table       = NULL;
	self->parts_count = 0;
	self->volume      = NULL;
	list_init(&self->parts);
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
	auto origin = deploy_find(&storage->deploy, self->lock.id->id);
	if (! self->origin)
	{
		unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
		return false;
	}
	self->origin = origin;

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

	// create partitions
	list_foreach_safe(&merger->objects)
	{
		auto object = list_at(Object, link);
		list_unlink(&object->link);
		auto part = part_allocate(object->source, &object->id,
		                          self->origin->seq,
		                          self->origin->unlogged);
		part->object = object;
		list_append(&self->parts, &part->link);
		self->parts_count++;
	}
	merger->objects_count = 0;
}

static void
refresh_apply(Refresh* self)
{
	auto storage = self->storage;
	auto table   = self->table;
	auto origin  = self->origin;

	// take table exclusive lock
	lock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// update volume mgr
		// remove origin
		// attach new partitions

	// update deploy hash
		// remove origin
		// add new partitions

	// handle delete

	// foreach partition
		// deploy send (without wait)

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// foreach partition
		// deploy recv
}

static void
refresh_end(Refresh* self)
{
	(void)self;
	// sync
	// undeploy directly (hash is updated)
	// remove origin object
	// free
}

void
refresh_run(Refresh* self, Id* id, Str* tier)
{
	// get catalog shared lock and partition lock
	storage_lock(self->storage, &self->lock, id);

	// find and rotate partition
	refresh_begin(self, id, tier);

	error_catch
	(
		// merge
		refresh_merge(self);

		// apply
		refresh_apply(self);

		// gc
		refresh_end(self);
	);

	storage_unlock(self->storage, &self->lock);
}
