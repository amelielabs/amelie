
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

static bool
engine_recover_storage(Engine* self, Level* level, TierStorage* storage)
{
	// <base>/storage/<id_tier_storage>
	char id[UUID_SZ];
	uuid_get(&storage->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (! fs_exists("%s", path))
	{
		tier_storage_mkdir(storage);
		return true;
	}

	// read directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("tier: directory '%s' open error", path);
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;

		// <id>.type[.incomplete]
		int64_t psn;
		auto state = id_of(entry->d_name, &psn);
		if (state == -1)
		{
			info("tier: skipping unknown file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
		state_psn_follow(psn);

		Id id;
		id_init(&id);
		id.id       = psn;
		id.id_table = *self->id_table;
		id.type     = state;
		id.storage  = storage;
		id.tier     = level->tier;
		switch (state) {
		case ID_SERVICE:
		{
			auto service = service_allocate();
			service_set_id(service, &id);
			engine_add(self, &service->id);
			break;
		}
		case ID_SERVICE_INCOMPLETE:
		{
			// remove incomplete file
			id_delete(&id, state);
			break;
		}
		case ID_RAM:
		{
			auto part = part_allocate(&id, self->seq, self->unlogged);
			engine_add(self, &part->id);
			break;
		}
		case ID_RAM_INCOMPLETE:
		case ID_RAM_SNAPSHOT:
		{
			// remove incomplete and snapshot files
			id_delete(&id, state);
			break;
		}
		case ID_PENDING_INCOMPLETE:
		case ID_PENDING_SNAPSHOT:
		{
			// remove incomplete and snapshot files
			id_delete(&id, state);
			break;
		}
		case ID_PENDING:
		{
			auto obj = object_allocate(&id);
			engine_add(self, &obj->id);
			break;
		}
		}
	}
	return false;
}

static bool
engine_recover_level(Engine* self, Level* level)
{
	auto bootstrap = false;
	list_foreach(&level->tier->storages)
	{
		auto storage = list_at(TierStorage, link);
		if (engine_recover_storage(self, level, storage))
			bootstrap = true;
	}
	return bootstrap;
}

static void
engine_create(Engine* self, int count)
{
	// create initial hash partitions
	if (count < 1 || count > PART_MAPPING_MAX)
		error("table has invalid partitions number");

	auto main = engine_main(self);

	// hash partition_max / hash partitions
	int range_max      = PART_MAPPING_MAX;
	int range_interval = range_max / count;
	int range_start    = 0;
	for (auto order = 0; order < count; order++)
	{
		// choose next storagee
		auto storage = tier_storage_next(main->tier);

		// create partition
		Id id;
		id_init(&id);
		id.id       = state_psn_next();
		id.id_table = *self->id_table;
		id.type     = ID_RAM;
		id.storage  = storage;
		id.tier     = main->tier;

		auto part = part_allocate(&id, self->seq, self->unlogged);
		engine_add(self, &part->id);

		// set hash range
		int range_step;
		auto is_last = (order == count - 1);
		if (is_last)
			range_step = range_max - range_start;
		else
			range_step = range_interval;
		if ((range_start + range_step) > range_max)
			range_step = range_max - range_start;

		part->heap->header->hash_min = range_start;
		part->heap->header->hash_max = range_start + range_step;
		range_start += range_step;

		// create <id>.ram.incomplete file
		File file;
		file_init(&file);
		defer(file_close, &file);
		heap_create(part->heap, &file, &part->id, ID_RAM_INCOMPLETE);

		// sync
		if (opt_int_of(&config()->storage_sync))
			file_sync(&file);

		// rename
		id_rename(&part->id, ID_RAM_INCOMPLETE, ID_RAM);
	}
}

static void
engine_recover_gc(Engine* self, uint8_t* pos)
{
	// remove all existing partitions and their files
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		int64_t psn;
		json_read_integer(&pos, &psn);
		auto id = engine_find(self, psn);
		if (! id)
			continue;
		id_delete(id, id->type);
		engine_remove(self, id);
		id_free(id);
	}
}

static void
engine_recover_service(Engine* self, Service* service)
{
	// read service file
	service_open(service, ID_SERVICE);

	// if any of the output files are missing (removed as incomplete before),
	// then remove the rest of them
	//
	// case: crash after service file sync/rename
	//
	auto pos = service->output.start;
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		int64_t psn;
		json_read_integer(&pos, &psn);
		auto id = engine_find(self, psn);
		if (id)
			continue;
		engine_recover_gc(self, service->output.start);
		goto done;
	}

	// output files considered valid, remove all remaining
	// input files
	//
	// case: crash after output files synced and renamed
	//
	engine_recover_gc(self, service->input.start);

done:
	// remove service file
	id_delete(&service->id, ID_SERVICE);

	// remove from the list
	engine_remove(self, &service->id);

	// done
	service_free(service);
}

void
engine_recover(Engine* self, int count)
{
	// read files
	auto bootstrap = false;
	list_foreach(&self->levels)
	{
		auto level = list_at(Level, link);
		if (engine_recover_level(self, level))
			bootstrap = true;
	}

	// create initial partitions
	if (bootstrap)
		engine_create(self, count);

	// recover service files
	auto main = engine_main(self);
	list_foreach_safe(&main->list_service)
	{
		auto service = list_at(Service, id.link);
		engine_recover_service(self, service);
	}

	// todo: sort pending objects by id

	// open pending objects
	list_foreach_safe(&main->list_pending)
	{
		auto obj = list_at(Object, id.link);
		object_open(obj, ID_PENDING, true);
	}
}
