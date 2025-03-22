
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>

static inline void
auth_cache_node_free(AuthCacheNode* self)
{
	str_free(&self->digest);
	am_free(self);
}

void
auth_cache_init(AuthCache* self)
{
	hashtable_init(&self->ht);
}

void
auth_cache_free(AuthCache* self)
{
	if (self->ht.count > 0)
	{
		auto index = (AuthCacheNode**)(self->ht.buf.start);
		for (int i = 0; i < self->ht.size; i++)
		{
			auto node = index[i];
			if (node == NULL)
				continue;
			auth_cache_node_free(node);
		}
	}
	hashtable_free(&self->ht);
	hashtable_init(&self->ht);
}

void
auth_cache_prepare(AuthCache* self)
{
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);
}

void
auth_cache_reset(AuthCache* self)
{
	auth_cache_free(self);
}

void
auth_cache_add(AuthCache* self, User* user, Str* digest,
               int64_t expire)
{
	auto node = (AuthCacheNode*)am_malloc(sizeof(AuthCacheNode));
	hashtable_node_init(&node->node);
	node->node.hash = hash_murmur3_32(str_u8(digest), str_size(digest), 0);
	node->user   = user;
	node->expire = expire;
	str_init(&node->digest);
	str_copy(&node->digest, digest);

	hashtable_reserve(&self->ht);
	hashtable_set(&self->ht, &node->node);
}

void
auth_cache_del(AuthCache* self, AuthCacheNode* node)
{
	hashtable_delete(&self->ht, &node->node);
}

hot static inline bool
auth_cache_cmp(HashtableNode* node, void* ptr)
{
	auto at = container_of(node, AuthCacheNode, node);
	return str_compare(&at->digest, ptr);
}

hot AuthCacheNode*
auth_cache_find(AuthCache* self, Str* digest)
{
	auto hash = hash_murmur3_32(str_u8(digest), str_size(digest), 0);
	auto node = hashtable_get(&self->ht, hash, auth_cache_cmp, digest);
	if (! node)
		return NULL;
	return container_of(node, AuthCacheNode, node);
}
