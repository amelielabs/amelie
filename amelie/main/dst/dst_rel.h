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

typedef struct DstRel DstRel;

enum
{
	DST_REL_TABLE,
	DST_REL_TABLE_VECTOR,
	DST_REL_INDEX,
	DST_REL_CLONE,
	DST_REL_TOPIC,
	DST_REL_SUBSCRIPTION,
	DST_REL_MAX
};

struct DstRel
{
	uint64_t  id;
	int       type;
	DstRel*   parent;
	List      indexes;
	int       indexes_count;
	List      subs;
	int       subs_count;
	List      clones;
	int       clones_count;
	Hashtable state;

	// subscription state
	union
	{
		struct
		{
			int64_t cdc_sum;
			int     cdc_count;
			int64_t step_cdc_sum;
			int     step_cdc_count;
		};
	};

	List      link_parent;
	List      link;
};

static inline DstRel*
dst_rel_allocate(DstRel* parent, uint64_t id, int type, int keys)
{
	auto self = (DstRel*)am_malloc(sizeof(DstRel));
	memset(self, 0, sizeof(*self));
	self->id            = id;
	self->type          = type;
	self->parent        = parent;
	self->indexes_count = 0;
	self->subs_count    = 0;
	self->clones_count  = 0;
	list_init(&self->indexes);
	list_init(&self->subs);
	list_init(&self->clones);
	hashtable_create(&self->state, keys * 2);
	list_init(&self->link_parent);
	list_init(&self->link);
	return self;
}

static inline void
dst_rel_free(DstRel* self)
{
	auto index = (Hashnode**)(self->state.buf.start);
	for (int i = 0; i < self->state.size; i++)
	{
		if (!index[i] || index[i] == HASHTABLE_DELETED)
			continue;
		auto key = container_of(index[i], DstKey, node);
		dst_key_free(key);
	}
	hashtable_free(&self->state);
	am_free(self);
}

hot static inline bool
dst_rel_cmp(Hashnode* node, void* ptr)
{
	auto at = container_of(node, DstKey, node);
	return at->key == *(uint64_t*)ptr;
}

static inline DstKey*
dst_rel_get(DstRel* self, uint64_t key)
{
	auto hash = hash_murmur3_32((uint8_t*)&key, sizeof(key), 0);
	auto node = hashtable_get(&self->state, hash, dst_rel_cmp, &key);
	if (! node)
		return NULL;
	return container_of(node, DstKey, node);
}

static inline void
dst_rel_set(DstRel* self, DstKey* key)
{
	key->node.hash = hash_murmur3_32( (uint8_t*)&key->key, sizeof(key->key), 0);
	hashtable_set(&self->state, &key->node);
}

static inline void
dst_rel_delete(DstRel* self, DstKey* key)
{
	hashtable_delete(&self->state, &key->node);
}

static inline void
dst_rel_copy(DstRel* self, DstRel* from)
{
	auto index = (Hashnode**)(from->state.buf.start);
	for (int i = 0; i < from->state.size; i++)
	{
		if (!index[i] || index[i] == HASHTABLE_DELETED)
			continue;
		auto key = container_of(index[i], DstKey, node);
		auto key_copy = dst_key_copy(key);
		dst_rel_set(self, key_copy);
	}
}

static inline void
dst_rel_cdc(DstRel* self, DstKey* key)
{
	list_foreach(&self->subs)
	{
		auto sub = list_at(DstRel, link_parent);
		sub->cdc_sum += key->key;
		sub->cdc_count++;
	}
}
