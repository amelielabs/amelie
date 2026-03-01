
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

void
tier_mgr_init(TierMgr* self, StorageMgr* storage_mgr, Keys* keys)
{
	self->storage_mgr = storage_mgr;
	self->keys        = keys;
	self->list_count  = 0;
	list_init(&self->list);
}

void
tier_mgr_free(TierMgr* self)
{
	// unref storages and free tiers
	list_foreach_safe(&self->list)
	{
		auto tier = list_at(Tier, link);
		tier_mgr_remove(self, tier);
	}
}

void
tier_mgr_open(TierMgr* self, List* tiers)
{
	// create and recover tiers
	list_foreach(tiers)
	{
		auto config = list_at(TierConfig, link);
		auto tier = tier_mgr_create(self, config);
		tier_open(tier, self->storage_mgr);
	}
}

void
tier_mgr_drop(TierMgr* self)
{
	// delete all objects and volumes
	list_foreach_safe(&self->list)
	{
		auto tier = list_at(Tier, link);
		tier_drop(tier);
	}
}

void
tier_mgr_truncate(TierMgr* self)
{
	// truncate all objects
	list_foreach_safe(&self->list)
	{
		auto tier = list_at(Tier, link);
		tier_truncate(tier);
	}
}

Tier*
tier_mgr_create(TierMgr* self, TierConfig* config)
{
	auto tier = tier_allocate(config, self->keys);
	list_append(&self->list, &tier->link);
	self->list_count++;
	return tier;
}

void
tier_mgr_remove(TierMgr* self, Tier* tier)
{
	// unref storages
	volume_mgr_unref(&tier->config->volumes);

	list_unlink(&tier->link);
	self->list_count--;

	tier_free(tier);
}

void
tier_mgr_remove_by(TierMgr* self, Str* name)
{
	auto tier = tier_mgr_find(self, name);
	if (tier)
		tier_mgr_remove(self, tier);
}

Tier*
tier_mgr_find(TierMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto tier = list_at(Tier, link);
		if (str_compare(&tier->config->name, name))
			return tier;
	}
	return NULL;
}

Tier*
tier_mgr_find_by(TierMgr* self, Volume* volume)
{
	list_foreach(&self->list)
	{
		auto tier = list_at(Tier, link);
		if (volume_mgr_has(&tier->config->volumes, volume))
			return tier;
	}
	return NULL;
}

Object*
tier_mgr_find_object(TierMgr* self, Tier** ref, uint64_t psn)
{
	list_foreach(&self->list)
	{
		auto tier = list_at(Tier, link);
		auto object = tier_find(tier, psn);
		if (object)
		{
			if (ref)
				*ref = tier;
			return object;
		}
	}
	return NULL;
}

Buf*
tier_mgr_list(TierMgr* self, Str* ref, int flags)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show object id on table
	if (ref)
	{
		int64_t psn;
		if (str_toint(ref, &psn) == -1)
			error("invalid object id");

		Tier* tier = NULL;
		auto object = tier_mgr_find_object(self, &tier, psn);
		if (! object)
			encode_null(buf);
		else
			object_status(object, buf, flags, &tier->config->name);
		return buf;
	}

	// show objects on table
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto tier = list_at(Tier, link);
		list_foreach(&tier->list)
		{
			auto object = list_at(Object, link);
			object_status(object, buf, flags, &tier->config->name);
		}
	}
	encode_array_end(buf);
	return buf;
}
