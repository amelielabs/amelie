
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

	// create shadow heap
	auto heap_shadow = heap_allocate();
	heap_create(heap_shadow);

	// take table exclusive lock
	lock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// find partition by id
	auto origin = volume_mgr_find(&table->volume_mgr, self->lock.id->id);
	if (! origin)
	{
		unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
		heap_free(heap_shadow);
		return false;
	}
	self->origin = origin;
	if (! self->volume)
		self->volume = origin->volume;

	// commit pending prepared transactions
	auto consensus = &origin->track.consensus;
	track_sync(&origin->track, consensus);
	self->origin_lsn = origin->track.lsn;

	// switch partition shadow heap and begin heap snapshot
	assert(! origin->heap_shadow);
	origin->heap_shadow = heap_shadow;
	heap_snapshot(origin->heap, heap_shadow, true);

	unlock(&storage->lock_mgr, &table->rel, LOCK_EXCLUSIVE);
	return true;
}

static void
refresh_merge_job(intptr_t* argv)
{
	auto self = (Refresh*)argv[0];

	// sort and merge heap with existing object, create new objects
	auto source = self->volume->tier->config;
	auto origin = self->origin;
	auto merger = &self->merger;
	MergerConfig config =
	{
		.origin = origin->object,
		.heap   = origin->heap,
		.lsn    = self->origin_lsn,
		.keys   = self->table->volume_mgr.mapping.keys,
		.source = source
	};
	merger_execute(merger, &config);

	// split
	if (merger->objects_count <= 1)
		return;

	// create new partitions
	list_foreach(&merger->objects)
	{
		auto object = list_at(Object, link);
		auto part = part_allocate(source, &object->id, origin->seq, origin->unlogged);
		buf_write(&self->split, &part, sizeof(Part**));

		// set object
		part->object = object;
		track_set_lsn(&part->track, object->meta.lsn);

		// create indexes
		for (auto index = origin->indexes; index; index = index->next)
			part_index_add(part, index->config);

		// load partition in memory, if necessary
		if (part->source->in_memory)
			part_load(part);
	}
	list_init(&merger->objects);
}

static void
refresh_apply_in_memory_gc_job(intptr_t* argv)
{
	auto heap          = (Heap*)argv[0];
	auto object        = (Object*)argv[1];
	auto object_origin = (Object*)argv[2];

	// free shadow heap
	heap_free(heap);

	// sync created object file
	file_sync(&object->file);

	// remove parent object file
	object_delete(object_origin, ID);
	object_free(object_origin);
}

static void
refresh_apply_in_memory(Refresh* self)
{
	auto lock_mgr      = &self->storage->lock_mgr;
	auto table         = self->table;
	auto origin        = self->origin;
	auto origin_object = self->origin->object;

	// take table exclusive lock
	lock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// update partition object
	auto object = merger_first(&self->merger);
	origin->id     = object->id;
	origin->source = object->source;
	origin->object = object;

	// update volume if it is changed
	if (origin->volume != self->volume)
	{
		volume_remove(origin->volume, origin);
		volume_add(self->volume, origin);
	}

	// snapshot complete
	auto heap_shadow = origin->heap_shadow;
	origin->heap_shadow = NULL;

	heap_snapshot(origin->heap, NULL, false);

	// apply updates during heap snapshot
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap_shadow, NULL);
	while (heap_iterator_has(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		if (chunk->is_shadow_free)
		{
			// delayed heap removal
			heap_remove(origin->heap, *(void**)chunk->data);
			continue;
		}

		// ignore delete rows since index is in sync
		auto row_shadow = (Row*)chunk->data;
		if (row_shadow->is_delete)
			continue;

		// copy row
		auto row = row_copy(origin->heap, row_shadow);

		// update indexes using row copy (replace shadow copy)
		part_apply(origin, row, false);
	}

	unlock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// cleanup
	//  free former shadow heap
	//  sync new object
	//  delete origin object
	run(refresh_apply_in_memory_gc_job, 3,
	    heap_shadow, object, origin_object);
}

static void
refresh_apply_disk_gc_job(intptr_t* argv)
{
	auto heap          = (Heap*)argv[0];
	auto indexes       = (Index*)argv[1];
	auto object        = (Object*)argv[2];
	auto object_origin = (Object*)argv[3];

	// free shadow heap
	heap_free(heap);

	// free indexes
	auto index = indexes;
	while (index)
	{
		auto next = index->next;
		index_free(index);
		index = next;
	}

	// sync created object file
	file_sync(&object->file);

	// remove parent object file
	object_delete(object_origin, ID);
	object_free(object_origin);
}

static void
refresh_apply_disk(Refresh* self)
{
	auto lock_mgr      = &self->storage->lock_mgr;
	auto table         = self->table;
	auto origin        = self->origin;
	auto origin_object = self->origin->object;

	// create a new set of empty indexes
	Index* indexes_origin = origin->indexes;
	Index* indexes        = NULL;
	Index* indexes_last   = NULL;
	for (auto ref = origin->indexes; ref; ref = ref->next)
	{
		Index* index;
		if (ref->config->type == INDEX_TREE)
			index = index_tree_allocate(ref->config, self);
		else
		if (ref->config->type == INDEX_HASH)
			index = index_hash_allocate(ref->config, self);
		else
			abort();
		if (indexes_last)
			indexes_last->next = index;
		else
			indexes = index;
		indexes_last = index;
	}

	// take table exclusive lock
	lock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// update partition object
	auto object = merger_first(&self->merger);
	origin->id     = object->id;
	origin->source = object->source;
	origin->object = object;

	// update volume if it is changed
	if (origin->volume != self->volume)
	{
		volume_remove(origin->volume, origin);
		volume_add(self->volume, origin);
	}

	// snapshot complete
	auto heap_origin = origin->heap;
	auto heap_shadow = origin->heap_shadow;
	heap_snapshot(origin->heap, NULL, false);

	// use shadow heap as main
	origin->heap = heap_shadow;
	origin->heap_shadow = NULL;

	// switch to new empty set of indexes
	origin->indexes = indexes;

	// rebuild indexes using shadow heap (to exclude main heap rows)
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap_shadow, NULL);
	while (heap_iterator_has(&it))
	{
		// translating to main rows
		auto chunk = heap_iterator_at_chunk(&it);
		assert(! chunk->is_shadow_free);
		chunk->is_shadow = false;

		auto row = heap_iterator_at(&it);
		part_apply(origin, row, false);
	}

	unlock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// cleanup
	//  free former main heap
	//  free former indexes
	//  sync new object
	//  delete origin object
	run(refresh_apply_disk_gc_job, 4,
	    heap_origin, indexes_origin, object, origin_object);
}

static void
refresh_apply_split_gc_job(intptr_t* argv)
{
	auto self = (Refresh*)argv[0];

	// sync created objects files
	auto ref = (Part**)self->split.start;
	auto end = (Part**)self->split.position;
	for (; ref < end; ref++)
	{
		auto part = *ref;
		file_sync(&part->object->file);
	}

	// remove parent object file
	object_delete(self->origin->object, ID);
	object_free(self->origin->object);

	// free partition
	part_free(self->origin);
	self->origin = NULL;
}

static void
refresh_apply_split(Refresh* self)
{
	auto lock_mgr = &self->storage->lock_mgr;
	auto table    = self->table;
	auto origin   = self->origin;

	// take table exclusive lock
	lock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// force commit prepared transactions
	track_sync(&origin->track, &origin->track.consensus);

	// detach old partition
	volume_mgr_remove(&table->volume_mgr, origin);
	auto mapping = &table->volume_mgr.mapping;
	mapping_remove(&table->volume_mgr.mapping, origin);
	deploy_detach(&self->storage->deploy, origin);

	// attach new partitions
	auto ref = (Part**)self->split.start;
	auto end = (Part**)self->split.position;
	for (; ref < end; ref++)
	{
		auto part = *ref;
		track_set_lsn(&part->track, origin->track.lsn);
		deploy_attach(&self->storage->deploy, origin);
		volume_mgr_add(&table->volume_mgr, self->volume, part);
		mapping_add(mapping, part);
	}
	buf_reset(&self->split);

	// apply updates during heap snapshot between new partitions
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, origin->heap_shadow, NULL);
	while (heap_iterator_has(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		if (chunk->is_shadow_free)
			continue;

		// map partition
		auto row_shadow = (Row*)chunk->data;
		auto part = mapping_map(mapping, row_shadow);

		// copy row
		if (row_shadow->is_delete)
		{
			// delete from index using delete row
			part_apply(origin, row_shadow, true);
		} else
		{
			// copy and update index
			auto row = row_copy(part->heap, row_shadow);
			part_apply(origin, row, false);
		}
	}

	// todo: send undeploy/deploy

	unlock(lock_mgr, &table->rel, LOCK_EXCLUSIVE);

	// todo: recv undeploy/deploy

	// cleanup
	//  sync new object
	//  delete origin object
	//  free origin partition (including heaps/indexes and object)
	run(refresh_apply_split_gc_job, 1, self);
}

static void
refresh_apply(Refresh* self)
{
	auto source = self->volume->tier->config;
	auto merger = &self->merger;

	// apply depending on the source type and
	// split/refresh operation
	void (*apply)(Refresh*) = NULL;

	// refresh
	if (merger->objects_count == 1)
	{
		if (source->in_memory)
			apply = refresh_apply_in_memory;
		else
			apply = refresh_apply_disk;
	} else
	{
		// split
		apply = refresh_apply_split;
	}
	apply(self);
}

void
refresh_init(Refresh* self, Storage* storage)
{
	part_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->volume     = NULL;
	self->table      = NULL;
	self->storage    = storage;
	buf_init(&self->split);
	merger_init(&self->merger);
}

void
refresh_free(Refresh* self)
{
	merger_free(&self->merger);
	buf_free(&self->split);
}

void
refresh_reset(Refresh* self)
{
	part_lock_init(&self->lock);
	self->origin     = NULL;
	self->origin_lsn = 0;
	self->volume     = NULL;
	self->table      = NULL;
	buf_reset(&self->split);
	merger_reset(&self->merger);
}

void
refresh_run(Refresh* self, Id* id, Str* tier)
{
	refresh_reset(self);

	// get catalog shared lock and partition service lock
	storage_lock(self->storage, &self->lock, id);

	// find and rotate partition
	refresh_begin(self, id, tier);

	// create 1-N new partitions by merging existing
	// object file with heap
	auto on_error = error_catch
	(
		// run merge in background
		run(refresh_merge_job, 1, self);

		// apply
		refresh_apply(self);
	);

	// unlock
	storage_unlock(self->storage, &self->lock);

	if (on_error)
		rethrow();
}
