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

typedef struct PartMgr PartMgr;

typedef void (*PartAttach)(PartList*, void*);

struct PartMgr
{
	Hashtable  ht;
	PartAttach attach;
	void*      attach_arg;
};

static inline void
part_mgr_init(PartMgr* self, PartAttach attach, void* attach_arg)
{
	self->attach     = attach;
	self->attach_arg = attach_arg;
	hashtable_init(&self->ht);
}

static inline void
part_mgr_free(PartMgr* self)
{
	hashtable_free(&self->ht);
}

static inline void
part_mgr_attach(PartMgr* self, PartList* list)
{
	// register partitions by id
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);

	list_foreach(&list->list)
	{
		auto part = list_at(Part, link);

		hashtable_reserve(&self->ht);
		uint32_t id = part->config->id;
		part->link_ht.hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
		hashtable_set(&self->ht, &part->link_ht);
	}

	// assign backends to partitions
	self->attach(list, self->attach_arg);
}

static inline void
part_mgr_detach(PartMgr* self, PartList* list)
{
	list_foreach(&list->list)
	{
		auto part = list_at(Part, link);
		hashtable_delete(&self->ht, &part->link_ht);
	}
}

hot static inline bool
part_mgr_cmp(HashtableNode* node, void* ptr)
{
	auto part = container_of(node, Part, link_ht);
	return part->config->id == *(uint32_t*)ptr;
}

hot static inline Part*
part_mgr_find(PartMgr* self, uint32_t id)
{
	auto hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
	auto node = hashtable_get(&self->ht, hash, part_mgr_cmp, &id);
	if (likely(node))
		return container_of(node, Part, link_ht);
	return NULL;
}
