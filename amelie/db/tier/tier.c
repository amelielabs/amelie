
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

static bool
tier_recover_volume(Tier* tier, Volume* volume)
{
	// <base>/storage/<volume_id>
	char id[UUID_SZ];
	uuid_get(&volume->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (! fs_exists("%s", path))
	{
		volume_mkdir(volume);
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
		id.id     = psn;
		id.type   = state;
		id.volume = volume;

		switch (state) {
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
			tier_add(tier, &obj->id);
			break;
		}
		default:
			info("tier: unexpected file: '%s%s'", path,
			     entry->d_name);
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

	// todo: sort pending objects by id

	// open pending objects
	list_foreach(&self->list_pending)
	{
		auto obj = list_at(Object, id.link);
		object_open(obj, ID_PENDING, true);
	}
}
