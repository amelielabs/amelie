
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

static bool
tier_open_storage(TierMgr* self, Tier* tier, TierStorage* storage)
{
	char id[UUID_SZ];
	uuid_get(&tier->config->id, id, sizeof(id));

	// <storage_path>/<tier>
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

		// add object to the tier
		Id id =
		{
			.id       = psn,
			.id_tier  = tier->config->id,
			.id_table = self->id_table,
			.state    = state,
			.storage  = storage->storage,
			.tier     = tier,
			.encoding = &tier->encoding
		};
		auto object = object_allocate(&id);
		tier_add(tier, object);
	}
	return false;
}

static bool
tier_open(TierMgr* self, Tier* tier)
{
	auto bootstrap = false;
	list_foreach(&tier->config->storages)
	{
		auto storage = list_at(TierStorage, link);
		if (tier_open_storage(self, tier, storage))
			bootstrap = true;
	}
	return bootstrap;
}

void
tier_mgr_init(TierMgr* self, StorageMgr* storage_mgr, Uuid* id_table)
{
	self->tiers       = NULL;
	self->tiers_count = 0;
	self->id_table    = *id_table;
	self->storage_mgr = storage_mgr;
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

bool
tier_mgr_open(TierMgr* self, List* tiers)
{
	auto  bootstrap = false;
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
		tier_resolve(tier, self->storage_mgr);

		// read objects
		if (tier_open(self, tier))
			bootstrap = true;
	}

	// ensure no objects for bootstrap
	if (bootstrap)
	{
		auto tier = self->tiers;
		for (; tier; tier = tier->next)
		{
			if (tier->list_count > 0)
				bootstrap = false;
		}
	}
	return bootstrap;
}

Buf*
tier_mgr_status(TierMgr* self, Str* ref, bool extended)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show tier name on table
	if (ref)
	{
		auto tier = tier_mgr_find(self, ref);
		if (! tier)
			encode_null(buf);
		else
			tier_status(tier, buf, extended);
		return buf;
	}

	// show tiers on table
	encode_array(buf);
	auto tier = self->tiers;
	for (; tier; tier = tier->next)
		tier_status(tier, buf, extended);
	encode_array_end(buf);
	return buf;
}

Tier*
tier_mgr_find(TierMgr* self, Str* name)
{
	auto tier = self->tiers;
	for (; tier; tier = tier->next)
	{
		if (str_compare(&tier->config->name, name))
			return tier;
	}
	return NULL;
}
