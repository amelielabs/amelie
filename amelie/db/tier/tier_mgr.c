
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
#include <amelie_object.h>
#include <amelie_tier.h>

static void
closedir_defer(DIR* self)
{
	closedir(self);
}

static void
tier_open_storage(Tier* self, TierStorage* storage)
{
	char id[UUID_SZ];
	uuid_get(&self->config->id, id, sizeof(id));

	// <storage_path>/<tier>
	char path[PATH_MAX];
	storage_fmt(storage->storage, path, "%s", id);

	// create tier directory, if not exists
	if (! fs_exists("%s", path))
	{
		fs_mkdir(0755, "%s", path);
		return;
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
		auto rc = id_of(entry->d_name, &psn);
		if (rc == -1)
		{
			info("tier: skipping unknown file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
		state_psn_follow(psn);

		// add object to the tier
		Id id =
		{
			.id      = psn,
			.id_tier = self->config->id
		};
		auto object = object_allocate(storage->storage, &id, &self->ec);
		tier_add(self, object);
	}
}

static void
tier_open(Tier* self)
{
	list_foreach(&self->config->storages)
	{
		auto storage = list_at(TierStorage, link);
		tier_open_storage(self, storage);
	}
}

void
tier_mgr_init(TierMgr* self)
{
	self->tiers = NULL;
	self->tiers_count = 0;
}

void
tier_mgr_free(TierMgr* self)
{
	auto tier = self->tiers;
	while (tier)
	{
		auto next = tier->next;
		tier_free(tier);
		tier = next;
	}
	self->tiers = NULL;
	self->tiers_count = 0;
}

void
tier_mgr_open(TierMgr* self, StorageMgr* storage_mgr, List* tiers)
{
	Tier* last = NULL;
	list_foreach(tiers)
	{
		// create tier
		auto config = list_at(TierConfig, link);
		auto tier = tier_allocate(config, NULL);
		if (last)
			last->next = tier;
		else
			self->tiers = tier;
		last = tier;
		self->tiers_count++;

		// resolve storages
		tier_resolve(tier, storage_mgr);

		// read objects
		tier_open(tier);
	}
}
