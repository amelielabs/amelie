
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
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>

static void
closedir_defer(DIR* self)
{
	closedir(self);
}

static bool
engine_recover_storage(Engine* self, Level* level, TierStorage* storage)
{
	// <storage_path>/<tier>
	char id[UUID_SZ];
	uuid_get(&level->tier->id, id, sizeof(id));

	char path[PATH_MAX];
	storage_pathfmt(storage->storage, path, "%s", id);

	// create tier directory, if not exists
	if (! fs_exists("%s", path))
	{
		fs_mkdir(0755, "%s", path);
		return true;
	}

	// read directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("tier: directory '%s' open error", path);
	defer(closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;

		// <id>.type [.incomplete]
		int64_t psn;
		auto state = id_of(entry->d_name, &psn);
		if (state == -1)
		{
			info("tier: skipping unknown file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
		state_psn_follow(psn);

		Id id =
		{
			.id       = psn,
			.id_tier  = level->tier->id,
			.id_table = *self->id_table,
			.storage  = storage->storage,
			.tier     = level->tier
		};
		switch (state) {
		case ID_RAM:
		{
			auto part = part_allocate(&id, self->seq, self->unlogged);
			level_add(level, part);
			break;
		}
		case ID_RAM_INCOMPLETE:
		{
			// remove incomplete file
			id_delete(&id, state);
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

	auto main = self->levels;
	auto main_storage = tier_storage(main->tier);

	// hash partition_max / hash partitions
	int range_max      = PART_MAPPING_MAX;
	int range_interval = range_max / count;
	int range_start    = 0;
	for (auto order = 0; order < count; order++)
	{
		// create partition
		Id id =
		{
			.id       = state_psn_next(),
			.id_tier  = main->tier->id,
			.id_table = *self->id_table,
			.storage  = main_storage->storage,
			.tier     = main->tier
		};
		auto part = part_allocate(&id, self->seq, self->unlogged);
		level_add(main, part);

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

void
engine_recover(Engine* self, int count)
{
	auto bootstrap = false;
	for (auto level = self->levels; level; level = level->next)
		if (engine_recover_level(self, level))
			bootstrap = true;
	if (bootstrap)
		engine_create(self, count);
}
