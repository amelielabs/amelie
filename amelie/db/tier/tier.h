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
	Encoding    encoding;
	Tier*       next;
};

static inline Tier*
tier_allocate(TierConfig* config, Keys* keys)
{
	auto self = (Tier*)am_malloc(sizeof(Tier));
	self->list_count = 0;
	self->config     = tier_config_copy(config);
	self->next       = NULL;
	mapping_init(&self->mapping, keys);
	list_init(&self->list);
	encoding_init(&self->encoding);

	// set encoding
	auto ec = &self->encoding;
	ec->compression       = &self->config->compression;
	ec->compression_level = self->config->compression_level;
	ec->encryption        = &self->config->encryption;
	ec->encryption_key    = &self->config->encryption_key;
	return self;
}

static inline void
tier_free(Tier* self)
{
	list_foreach_safe(&self->list)
	{
		auto object = list_at(Object, link);
		object_free(object);
	}
	tier_config_free(self->config);
	am_free(self);
}

static inline void
tier_add(Tier* self, Object* object)
{
	list_append(&self->list, &object->link);
	self->list_count++;
}

static inline void
tier_remove(Tier* self, Object* object)
{
	assert(object->id.tier == self);
	list_unlink(&object->link);
	self->list_count--;
}

static inline Storage*
tier_storage(Tier* self)
{
	return container_of(self->config->storages.next, TierStorage, link)->storage;
}

static inline void
tier_resolve(Tier* self, StorageMgr* storage_mgr)
{
	list_foreach(&self->config->storages)
	{
		auto ref = list_at(TierStorage, link);
		ref->storage = storage_mgr_find(storage_mgr, &ref->name, true);
	}
}

static inline void
tier_status(Tier* self, Buf* buf, bool extended)
{
	unused(extended);
	tier_config_write(self->config, buf, false);
}
