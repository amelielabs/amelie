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
	DST_REL_TOPIC,
	DST_REL_MAX
};

struct DstRel
{
	int       type;
	Hashtable state;

	// cdc
	int64_t   cdc_sum;
	int       cdc_count;
};

static inline void
dst_rel_init(DstRel* self, int type)
{
	memset(self, 0, sizeof(*self));
	self->type = type;
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
dst_rel_cdc(DstRel* self, DstKey* key)
{
	self->cdc_sum += key->key;
	self->cdc_count++;
}
