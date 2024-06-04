#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct PartMgr PartMgr;

struct PartMgr
{
	Hashtable ht;
};

static inline void
part_mgr_init(PartMgr* self)
{
	hashtable_init(&self->ht);
}

static inline void
part_mgr_free(PartMgr* self)
{
	hashtable_free(&self->ht);
}

static inline void
part_mgr_add(PartMgr* self, Part* part)
{
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);
	hashtable_reserve(&self->ht);

	uint32_t id = part->config->id;
	part->link_ht.hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
	hashtable_set(&self->ht, &part->link_ht);
}

static inline void
part_mgr_del(PartMgr* self, Part* part)
{
	hashtable_delete(&self->ht, &part->link_ht);
}

hot static inline bool
part_mgr_cmp(HashtableNode* node, void* ptr)
{
	auto part = container_of(node, Part, link_ht);
	return part->config->id == *(uint32_t*)ptr;
}

static inline Part*
part_mgr_find(PartMgr* self, uint32_t id)
{
	auto hash = hash_murmur3_32((uint8_t*)&id, sizeof(id), 0);
	auto node = hashtable_get(&self->ht, hash, part_mgr_cmp, &id);
	if (likely(node))
		return container_of(node, Part, link_ht);
	return NULL;
}
