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

typedef struct DeployIf DeployIf;
typedef struct Deploy   Deploy;

struct DeployIf
{
	void (*attach)(Deploy*, VolumeMgr*);
	void (*detach)(Deploy*, VolumeMgr*);
};

struct Deploy
{
	Hashtable ht;
	DeployIf* iface;
	void*     iface_arg;
};

static inline void
deploy_init(Deploy* self, DeployIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	hashtable_init(&self->ht);
}

static inline void
deploy_free(Deploy* self)
{
	hashtable_free(&self->ht);
}

static inline void
deploy_attach(Deploy* self, VolumeMgr* mgr)
{
	// register partitions by id
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);

	list_foreach(&mgr->parts)
	{
		auto part = list_at(Part, link);
		hashtable_reserve(&self->ht);
		part->link_hash.hash = hash_murmur3_32((uint8_t*)&part->id.id, sizeof(part->id.id), 0);
		hashtable_set(&self->ht, &part->link_hash);
	}

	// create pods
	self->iface->attach(self, mgr);
}

static inline void
deploy_detach(Deploy* self, VolumeMgr* mgr)
{
	list_foreach(&mgr->parts)
	{
		auto part = list_at(Part, link);
		hashtable_delete(&self->ht, &part->link_hash);
	}

	// drop pods
	self->iface->detach(self, mgr);
}

hot static inline bool
deploy_cmp(HashtableNode* node, void* ptr)
{
	auto part = container_of(node, Part, link_hash);
	return !memcmp(&part->id.id, ptr, sizeof(uint64_t));
}

hot static inline Part*
deploy_find(Deploy* self, uint64_t psn)
{
	auto hash = hash_murmur3_32((uint8_t*)&psn, sizeof(psn), 0);
	auto node = hashtable_get(&self->ht, hash, deploy_cmp, &psn);
	if (likely(node))
		return container_of(node, Part, link_hash);
	return NULL;
}
