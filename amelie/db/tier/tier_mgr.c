
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
	// create tiers
	list_foreach(tiers)
	{
		auto config = list_at(TierConfig, link);
		tier_mgr_create(self, config);
	}

	// recover tiers and create object lists
	tier_mgr_recover(self);
}

void
tier_mgr_load(TierMgr* self)
{
	// open pending objects
	list_foreach_safe(&self->list)
	{
		auto tier = list_at(Tier, link);
		list_foreach_safe(&tier->list_pending)
		{
			auto obj = list_at(Object, id.link);
			object_open(obj, ID_PENDING, true);
		}
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

	// resolve storages
	volume_mgr_ref(&tier->config->volumes, self->storage_mgr);
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

Id*
tier_mgr_find_object(TierMgr* self, Tier** ref, uint64_t psn)
{
	list_foreach(&self->list)
	{
		auto tier = list_at(Tier, link);
		auto id = tier_find(tier, psn);
		if (id)
		{
			if (ref)
				*ref = tier;
			return id;
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
		auto id = tier_mgr_find_object(self, &tier, psn);
		if (! id)
			encode_null(buf);
		else
			object_status(object_of(id), buf, flags, &tier->config->name);
		return buf;
	}

	// show objects on table
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto tier = list_at(Tier, link);
		list_foreach(&tier->list_pending)
		{
			auto obj = list_at(Object, id.link);
			object_status(obj, buf, flags, &tier->config->name);
		}
	}
	encode_array_end(buf);
	return buf;
}
