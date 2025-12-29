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

typedef struct VaultIf VaultIf;
typedef struct Vault   Vault;

struct VaultIf
{
	void (*attach)(Vault*, PartMgr*);
	void (*detach)(Vault*, PartMgr*);
};

struct Vault
{
	Hashtable ht;
	VaultIf*  iface;
	void*     iface_arg;
};

static inline void
vault_init(Vault* self, VaultIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	hashtable_init(&self->ht);
}

static inline void
vault_free(Vault* self)
{
	hashtable_free(&self->ht);
}

static inline void
vault_attach(Vault* self, PartMgr* mgr)
{
	// register partitions by id
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);

	list_foreach(&mgr->list)
	{
		auto part = list_at(Part, link);

		hashtable_reserve(&self->ht);
		uint32_t id = part->config->id;
		part->link_vault.hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
		hashtable_set(&self->ht, &part->link_vault);
	}

	// create pods
	self->iface->attach(self, mgr);
}

static inline void
vault_detach(Vault* self, PartMgr* mgr)
{
	list_foreach(&mgr->list)
	{
		auto part = list_at(Part, link);
		hashtable_delete(&self->ht, &part->link_vault);
	}

	// drop pods
	self->iface->detach(self, mgr);
}

hot static inline bool
vault_cmp(HashtableNode* node, void* ptr)
{
	auto part = container_of(node, Part, link_vault);
	return part->config->id == *(uint32_t*)ptr;
}

hot static inline Part*
vault_find(Vault* self, uint32_t id)
{
	auto hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
	auto node = hashtable_get(&self->ht, hash, vault_cmp, &id);
	if (likely(node))
		return container_of(node, Part, link_vault);
	return NULL;
}
