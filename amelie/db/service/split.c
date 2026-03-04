
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
split_begin(Split* self, Table* table, uint64_t id, bool refresh)
{
	self->table   = table;
	self->refresh = refresh;

	// take table shared lock
	auto lock_table = lock(&table->rel, LOCK_SHARED);
	defer(unlock, lock_table);

	// find object
	list_foreach(&table->tier_mgr.list)
	{
		auto tier   = list_at(Tier, link);
		auto object = tier_find(tier, id);
		if (object)
		{
			self->origin = object;
			self->tier   = tier;
			break;
		}
	}
	if (! self->origin)
		return false;

	// allow refresh only with 1 branch
	if (!refresh && self->origin->branches_count > 1)
		error("split: object has more then one branch");

	return true;
}

static Object*
split_create(Split* self)
{
	// create new object
	Id id =
	{
		.id      = state_psn_next(),
		.version = 1,
		.volume  = volume_mgr_next(&self->tier->config->volumes)
	};
	auto object = object_allocate(&id);
	buf_write(&self->objects, &object, sizeof(Object**));
	self->objects_count++;

	// create root branch
	auto branch = branch_allocate(&object->id, &object->file);
	object_add(object, branch);

	// create <id>.<version>.incomplete file
	object_create(object, STATE_INCOMPLETE);
	return object;
}

static inline void
split_write(Split* self, Iterator* it, Object* object, uint64_t limit)
{
	// start writer
	auto writer = self->writer;
	writer_reset(writer);
	writer_start(writer, &object->file, object->id.volume->storage,
	             self->tier->config->region_size);
	while (iterator_has(it))
	{
		auto row = iterator_at(it);
		if (row->is_delete)
		{
			iterator_next(it);
			continue;
		}
		writer_add(writer, row);
		if (writer_is_limit(writer, limit))
			break;
		iterator_next(it);
	}
	writer_stop(writer);

	// set branch meta data
	meta_writer_copy(&writer->meta_writer,
	                 &object->root->meta,
	                 &object->root->meta_data);

	auto id    = &object->id;
	auto total = (double)object->file.size / 1024 / 1024;
	info("split: %s/%s/%05" PRIu64 ".%02" PRIu64 " (%.2f MiB)",
	     id->volume->storage->config->name.pos,
	     self->tier->config->name.pos,
	     id->id,
	     id->version,
	     total);
}

static void
split_refresh_job(intptr_t* argv)
{
	auto self = (Split*)argv[0];
	auto keys = table_keys(self->table);

	// create merge iterator and include all branches
	MergeIterator it;
	merge_iterator_init(&it);
	defer(merge_iterator_free, &it);
	auto branch = self->origin->branches;
	for (; branch; branch = branch->next)
	{
		auto branch_it = branch_iterator_allocate();
		merge_iterator_add(&it, &branch_it->it);
		branch_iterator_open(branch_it, keys, branch, NULL);
	}
	merge_iterator_open(&it, keys);

	// no data (delete after merge)
	if (! merge_iterator_has(&it))
		return;

	// create and write new object
	auto object = split_create(self);
	split_write(self, &it.it, object, UINT64_MAX);
}

static void
split_job(intptr_t* argv)
{
	auto self = (Split*)argv[0];
	auto keys = table_keys(self->table);

	// create merge iterator and include all branches
	auto it = branch_iterator_allocate();
	defer(branch_iterator_free, it);
	branch_iterator_open(it, keys, self->origin->root, NULL);

	// no data (delete after merge)
	if (! branch_iterator_has(it))
		return;

	// split into 1 or N new objects
	auto object_size = (size_t)self->tier->config->object_size;
	auto left = (size_t)self->origin->file.size;
	while (branch_iterator_has(it))
	{
		auto object = split_create(self);
		uint64_t limit = object_size;
		if (left > object_size)
			limit = object_size / 2;
		split_write(self, &it->it, object, limit);
		left -= object->file.size;
	}
}

static void
split_complete_job(intptr_t* argv)
{
	auto self = (Split*)argv[0];
	auto objects = (Object**)self->objects.start;	

	// create <id>.service.incomplete file
	auto service = self->service_file;
	service_file_begin(service);
	service_file_add_origin(service, &self->origin->id);
	for (auto at = 0; at < self->objects_count; at++)
		service_file_add_create(service, &objects[at]->id);
	service_file_end(service);
	service_file_create(service);

	// sync and rename objects
	for (auto at = 0; at < self->objects_count; at++)
	{
		// sync object file
		auto object = objects[at];
		if (opt_int_of(&config()->storage_sync))
			file_sync(&object->file);

		// rename
		object_rename(object, STATE_INCOMPLETE, STATE_COMPLETE);
	}

	// unlink origin object file
	object_delete(self->origin, STATE_COMPLETE);

	// remove service file (done)
	service_file_delete(service);
}

static void
split_apply(Split* self)
{
	auto table  = self->table;
	auto origin = self->origin;

	// take table exclusive lock
	auto lock_table = lock(&table->rel, LOCK_EXCLUSIVE);
	defer(unlock, lock_table);

	// remove original object from tier
	tier_remove(self->tier, origin);

	// add new objects
	auto objects = (Object**)self->objects.start;	
	for (auto at = 0; at < self->objects_count; at++)
		tier_add(self->tier, objects[at]);
}

void
split_init(Split* self, Service* service)
{
	self->origin        = NULL;
	self->objects_count = 0;
	self->writer        = writer_allocate();
	self->refresh       = false;
	self->table         = NULL;
	self->tier          = NULL;
	self->service_file  = service_file_allocate();
	self->service       = service;
	service_lock_init(&self->lock);
	buf_init(&self->objects);
}

void
split_free(Split* self)
{
	split_reset(self);
	writer_free(self->writer);
	service_file_free(self->service_file);
	buf_free(&self->objects);
}

void
split_reset(Split* self)
{
	self->origin        = NULL;
	self->objects_count = 0;
	self->table         = NULL;
	self->tier          = NULL;
	service_file_reset(self->service_file);
	service_lock_init(&self->lock);
	writer_reset(self->writer);
	buf_reset(&self->objects);
}

bool
split_run(Split* self, Table* table, uint64_t id, bool refresh)
{
	split_reset(self);

	// lock object by id
	service_lock(self->service, &self->lock, LOCK_EXCLUSIVE, id);
	defer(service_unlock, &self->lock);

	// find object
	if (! split_begin(self, table, id, refresh))
		return false;

	auto on_error = error_catch
	(
		// run split in background
	 	if (refresh)
			run(split_refresh_job, 1, self);
		else
			run(split_job, 1, self);

		// apply
		split_apply(self);

		// finilize and cleanup
		run(split_complete_job, 1, self);
	);

	// todo: abort

	if (on_error)
		rethrow();

	return true;
}
