
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

Tier*
tier_allocate(TierConfig* config, Keys* keys)
{
	auto self = (Tier*)am_malloc(sizeof(Tier));
	self->list_count = 0;
	self->config     = config;
	mapping_init(&self->mapping, keys);
	list_init(&self->list);
	list_init(&self->link);
	return self;
}

void
tier_free(Tier* self)
{
	list_foreach_safe(&self->list)
	{
		auto object = list_at(Object, link);
		object_free(object);
	}
	am_free(self);
}

void
tier_add(Tier* self, Object* object)
{
	list_append(&self->list, &object->link);
	self->list_count++;
	volume_pin(object->id.volume);
}

void
tier_remove(Tier* self, Object* object)
{
	self->list_count--;
	list_unlink(&object->link);
}

void
tier_truncate(Tier* self)
{
	list_foreach_safe(&self->list)
	{
		auto object = list_at(Object, link);
		tier_remove(self, object);
		object_delete(object, STATE_COMPLETE);
		object_free(object);
	}
	self->list_count = 0;
	list_init(&self->list);
}

void
tier_drop(Tier* self)
{
	// delete objects
	tier_truncate(self);

	// delete volumes
	volume_mgr_rmdir(&self->config->volumes);
}

Object*
tier_find(Tier* self, uint64_t psn)
{
	list_foreach_safe(&self->list)
	{
		auto object = list_at(Object, link);
		if (object->id.id == psn)
			return object;
	}
	return NULL;
}

static void
tier_create(Volume* volume)
{
	// create volume directory
	volume_mkdir(volume);

	// create <id>.1.object.incomplete file
	Id id =
	{
		.id      = state_psn_next(),
		.version = 1,
		.volume  = volume,
	};
	auto object = object_allocate(&id);
	defer(object_free, object);
	object_create(object, STATE_INCOMPLETE);

	// create empty branch
	auto writer = writer_allocate();
	defer(writer_free, writer);
	writer_start(writer, &object->file, volume->storage, 0);
	writer_stop(writer);

	// sync incomplete file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&object->file);

	// rename
	object_rename(object, STATE_INCOMPLETE, STATE_COMPLETE);
}

static void
tier_open_volume(Tier* self, Volume* volume)
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

		// <id>.<version>[.state]
		int64_t psn     = 0;
		int64_t version = 0;
		auto    state   = id_of(entry->d_name, &psn, &version);
		if (state == -1 || !version)
		{
			info("tier: skipping unknown file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
		state_psn_follow(psn);

		Id id =
		{
			.id      = psn,
			.version = version,
			.volume  = volume
		};
		switch (state) {
		case STATE_COMPLETE:
		{
			// add object to the tier
			auto object = object_allocate(&id);
			tier_add(self, object);

			// recover branches
			object_open(object, STATE_COMPLETE);
			break;
		}
		case STATE_INCOMPLETE:
		case STATE_SNAPSHOT:
		{
			// remove incomplete and snapshot files
			id_delete(&id, state);
			break;
		}
		}
	}
}

void
tier_open(Tier* self, StorageMgr* storage_mgr)
{
	// resolve storages
	volume_mgr_ref(&self->config->volumes, storage_mgr);

	// recover volumes
	list_foreach(&self->config->volumes.list)
	{
		auto volume = list_at(Volume, link);
		tier_open_volume(self, volume);
	}
}
