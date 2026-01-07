
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
#include <amelie_object.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_tier.h>

static inline char*
recover_idof(char* path, uint64_t* id)
{
	*id = 0;
	while (*path && *path != '.')
	{
		if (unlikely(! isdigit(*path)))
			return NULL;
		*id = (*id * 10) + *path - '0';
		path++;
	}
	return path;
}

static inline int
recover_id(char* path, Id* id)
{
	// <id_parent>.<id>
	uint64_t id_self = 0;
	uint64_t id_parent = 0;
	path = recover_idof(path, &id_self);
	if (!path || *path != '.')
		return -1;
	// .
	path++;
	// id
	path = recover_idof(path, &id_parent);
	if (!path || *path)
		return -1;
	id->id        = id_self;
	id->id_parent = id_parent;
	return 0;
}

static void
closedir_defer(DIR* self)
{
	closedir(self);
}

static bool
volume_mgr_recover_volume(VolumeMgr* self, Volume* volume)
{
	auto source = volume->tier->config;

	// create volume directory, if not exists
	char path[PATH_MAX];
	source_path(source, path, &self->id, "");
	if (! fs_exists("%s", path))
	{
		fs_mkdir(0755, "%s", path);
		return true;
	}

	// read volume directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("volume: directory '%s' open error", path);
	defer(closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;

		// <id_parent>.<id>
		Id id;
		id_init(&id);
		if (recover_id(entry->d_name, &id) == -1)
		{
			info("volume: skipping unknown file: '%s%s'",
			     path, entry->d_name);
			continue;
		}

		// restore partition sequence number
		state_psn_follow(id.id);
		state_psn_follow(id.id_parent);

		// ensure partition id is unique
		auto part = volume_mgr_find(self, id.id);
		if (part)
			error("volume: duplicate partition id: '%s%s'",
			      path, entry->d_name);

		// create partition
		part = part_allocate(source, &id, self->seq, self->unlogged);

		// register partition
		volume_mgr_add(self, volume, part);
	}

	return false;
}

static void
volume_mgr_create(VolumeMgr* self, Volume* volume)
{
	auto source = volume->tier->config;

	// create initial hash partitions
	auto n = self->mapping_hash;
	if (n < 1 || n > UINT16_MAX)
		error("table has invalid partitions number");

	auto writer = writer_allocate();
	defer(writer_free, writer);

	File file;
	file_init(&file);
	defer(file_close, &file);

	// hash partition_max / hash partitions
	int range_max      = UINT16_MAX;
	int range_interval = range_max / n;
	int range_start    = 0;
	for (auto order = 0; order < n; order++)
	{
		// set hash range
		int range_step;
		auto is_last = (order == n - 1);
		if (is_last)
			range_step = range_max - range_start;
		else
			range_step = range_interval;
		if ((range_start + range_step) > range_max)
			range_step = range_max - range_start;
		auto hash_min = range_start;
		auto hash_max = range_start + range_step;

		// create object file
		Id id =
		{
			.id_parent = 0,
			.id        = state_psn_next(),
			.id_table  = self->id
		};
		object_file_create(&file, source, &id, ID);
		writer_reset(writer);
		writer_start(writer, volume->tier->config, &file);
		writer_stop(writer, &id, hash_min, hash_max, 0, 0);
		file_close(&file);

		// create and register partition
		auto part = part_allocate(source, &id, self->seq, self->unlogged);
		volume_mgr_add(self, volume, part);
		if (is_last)
			break;

		range_start += range_step;
	}
}

void
volume_mgr_recover(VolumeMgr* self, List* volumes, List* indexes)
{
	// prepare volumes
	auto create = false;
	list_foreach(volumes)
	{
		auto config = list_at(VolumeConfig, link);

		// find tier
		auto tier = tier_mgr_find(self->tier_mgr, &config->tier, true);

		// add volume
		auto volume = volume_allocate(config, tier);
		list_append(&self->volumes, &volume->link);
		self->volumes_count++;

		// open or create volume
		if (volume_mgr_recover_volume(self, volume))
			create = true;
	}

	// maybe create initial partitions
	if (create)
	{
		auto main = container_of(list_first(volumes), Volume, link);
		volume_mgr_create(self, main);
	}

	// validate and open partitions
	list_foreach_safe(&self->parts)
	{
		auto part = list_at(Part, link);

		// drop partition if the parent partition still exists
		if (volume_mgr_find(self, part->id.id_parent))
		{
			volume_mgr_remove(self, part);
			object_file_delete(part->source, &part->id, ID);
			part_free(part);
		}

		// open partition
		part->object = object_allocate(part->source, &part->id);
		object_open(part->object, ID, true);

		// add indexes
		list_foreach(indexes)
		{
			auto config = list_at(IndexConfig, link);
			part_index_add(part, config);
		}

		// add partition mapping
		mapping_add(&self->mapping, part);
	}
}
