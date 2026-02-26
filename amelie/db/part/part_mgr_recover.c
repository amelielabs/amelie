
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

static void
part_mgr_recover_volume(PartMgr* self, Volume* volume)
{
	// <base>/storage/<volume_id>
	char id[UUID_SZ];
	uuid_get(&volume->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (! fs_exists("%s", path))
	{
		volume_mkdir(volume);
		return;
	}

	// read directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("part_mgr: directory '%s' open error", path);
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
			info("part_mgr: skipping unknown file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
		state_psn_follow(psn);

		Id id;
		id_init(&id);
		id.id     = psn;
		id.type   = state;
		id.volume = volume;

		switch (state) {
		case ID_PART_INCOMPLETE:
		case ID_PART_SNAPSHOT:
		{
			// remove incomplete and snapshot files
			id_delete(&id, state);
			break;
		}
		case ID_PART:
		{
			auto part = part_allocate(&id, self->arg);
			part_mgr_add(self, &part->id);
			break;
		}
		default:
			info("part_mgr: unexpected file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
	}
}

static void
part_mgr_create(PartMgr* self)
{
	// create initial hash partitions
	auto count = self->config->partitions;
	if (count < 1 || count > PART_MAPPING_MAX)
		error("table has invalid partitions number");

	// hash partition_max / hash partitions
	int range_max      = PART_MAPPING_MAX;
	int range_interval = range_max / count;
	int range_start    = 0;
	for (auto order = 0; order < count; order++)
	{
		// choose next volume
		auto volume = volume_mgr_next(&self->config->volumes);

		// create partition
		Id id;
		id_init(&id);
		id.id       = state_psn_next();
		id.type     = ID_PART;
		id.volume   = volume;
		auto part = part_allocate(&id, self->arg);
		part_mgr_add(self, &part->id);

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

		// create <id>.partition.incomplete file
		File file;
		file_init(&file);
		defer(file_close, &file);
		heap_create(part->heap, &file, &part->id, ID_PART_INCOMPLETE);

		// sync
		if (opt_int_of(&config()->storage_sync))
			file_sync(&file);

		// rename
		id_rename(&part->id, ID_PART_INCOMPLETE, ID_PART);
	}
}

void
part_mgr_recover(PartMgr* self)
{
	// resolve storages
	volume_mgr_ref(&self->config->volumes, self->tier_mgr->storage_mgr);

	// read files
	list_foreach(&self->config->volumes.list)
	{
		auto volume = list_at(Volume, link);
		part_mgr_recover_volume(self, volume);
	}

	// create initial partitions
	if (! self->list_count)
		part_mgr_create(self);
}
