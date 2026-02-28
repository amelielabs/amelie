#pragma once

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

typedef struct Tier Tier;

struct Tier
{
	Mapping     mapping;
	List        list;
	int         list_count;
	TierConfig* config;
	List        link;
};

static inline Tier*
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

static inline void
tier_free(Tier* self)
{
	list_foreach_safe(&self->list)
	{
		auto id = list_at(Id, link);
		id_free(id);
	}
	am_free(self);
}

static inline bool
tier_empty(Tier* self)
{
	return !self->list_count;
}

static inline void
tier_add(Tier* self, Id* id)
{
	list_append(&self->list, &id->link);
	self->list_count++;
	volume_ref(id->volume);
}

static inline void
tier_remove(Tier* self, Id* id)
{
	self->list_count--;
	list_unlink(&id->link);
	volume_unref(id->volume);
}

static inline void
tier_truncate(Tier* self)
{
	list_foreach_safe(&self->list)
	{
		auto id = list_at(Id, link);
		tier_remove(self, id);
		id_delete(id, id->type);
		id_free(id);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
tier_drop(Tier* self)
{
	// delete objects
	tier_truncate(self);

	// delete volumes
	volume_mgr_rmdir(&self->config->volumes);
}

static inline Id*
tier_find(Tier* self, uint64_t psn)
{
	list_foreach_safe(&self->list)
	{
		auto id = list_at(Id, link);
		if (id->id == psn)
			return id;
	}
	return NULL;
}

void tier_recover(Tier*, StorageMgr*);
