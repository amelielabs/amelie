
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

static void
tier_create(Volume* volume)
{
	// create volume directory
	volume_mkdir(volume);

	// create empty object

	// create <id>.object.incomplete file
	Id id;
	id_init(&id);
	id.id     = state_psn_next();
	id.type   = ID_OBJECT;
	id.volume = volume;

	File file;
	file_init(&file);
	defer(file_close, &file);
	id_create(&id, &file, ID_OBJECT_INCOMPLETE);

	auto writer = writer_allocate();
	defer(writer_free, writer);
	writer_start(writer, &file, volume->storage, 0);
	writer_stop(writer);

	// sync incomplete file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	file_close(&file);

	// rename
	id_rename(&id, ID_OBJECT_INCOMPLETE, ID_OBJECT);
}

static bool
tier_recover_volume(Tier* self, Volume* volume)
{
	// <base>/storage/<volume_id>
	char id[UUID_SZ];
	uuid_get(&volume->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (! fs_exists("%s", path))
		tier_create(volume);

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
		id.id     = psn;
		id.type   = state;
		id.volume = volume;

		switch (state) {
		case ID_OBJECT_INCOMPLETE:
		case ID_OBJECT_SNAPSHOT:
		{
			// remove incomplete and snapshot files
			id_delete(&id, state);
			break;
		}
		case ID_OBJECT:
		{
			auto obj = object_allocate(&id);
			tier_add(self, &obj->id);
			break;
		}
		default:
			info("tier: unexpected file: '%s%s'", path, entry->d_name);
			continue;
		}
	}
	return false;
}

void
tier_recover(Tier* self, StorageMgr* storage_mgr)
{
	// resolve storages
	volume_mgr_ref(&self->config->volumes, storage_mgr);

	// recover volumes
	list_foreach(&self->config->volumes.list)
	{
		auto volume = list_at(Volume, link);
		tier_recover_volume(self, volume);
	}

	// open files objects
	list_foreach(&self->list)
	{
		auto obj = list_at(Object, id.link);
		object_open(obj, ID_OBJECT, true);

		// branch
		if (obj->meta.parent != 0)
		{
			auto parent = tier_find(self, obj->meta.parent);
			if (! parent)
				error("tier: parent object %" PRIu64 " not found",
				      obj->meta.parent);

			// branches are ordered by id (highest first)
			object_attach(object_of(parent), obj);
		}
	}
}
