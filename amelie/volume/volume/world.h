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

typedef struct WorldIf WorldIf;
typedef struct World   World;

struct WorldIf
{
	void (*attach)(World*, VolumeMgr*);
	void (*detach)(World*, VolumeMgr*);
};

struct World
{
	Hashtable ht;
	WorldIf*  iface;
	void*     iface_arg;
};

static inline void
world_init(World* self, WorldIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	hashtable_init(&self->ht);
}

static inline void
world_free(World* self)
{
	hashtable_free(&self->ht);
}

static inline void
world_attach(World* self, VolumeMgr* mgr)
{
	// register partitions by id
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);

	list_foreach(&mgr->list_parts)
	{
		auto part = list_at(Part, link);

		hashtable_reserve(&self->ht);
		uint32_t id = part->config->id;
		part->link_hash.hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
		hashtable_set(&self->ht, &part->link_hash);
	}

	// create pods
	self->iface->attach(self, mgr);
}

static inline void
world_detach(World* self, VolumeMgr* mgr)
{
	list_foreach(&mgr->list_parts)
	{
		auto part = list_at(Part, link);
		hashtable_delete(&self->ht, &part->link_hash);
	}

	// drop pods
	self->iface->detach(self, mgr);
}

hot static inline bool
world_cmp(HashtableNode* node, void* ptr)
{
	auto part = container_of(node, Part, link_hash);
	return part->config->id == *(uint32_t*)ptr;
}

hot static inline Part*
world_find(World* self, uint32_t id)
{
	auto hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
	auto node = hashtable_get(&self->ht, hash, world_cmp, &id);
	if (likely(node))
		return container_of(node, Part, link_hash);
	return NULL;
}
