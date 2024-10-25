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

typedef struct AuthCacheNode AuthCacheNode;
typedef struct AuthCache     AuthCache;

struct AuthCacheNode
{
	HashtableNode node;
	User*         user;
	int64_t       expire;
	Str           digest;
};

struct AuthCache
{
	Hashtable ht;
};

void  auth_cache_init(AuthCache*);
void  auth_cache_free(AuthCache*);
void  auth_cache_reset(AuthCache*);
void  auth_cache_prepare(AuthCache*);
void  auth_cache_add(AuthCache*, User*, Str*, int64_t);
void  auth_cache_del(AuthCache*, AuthCacheNode*);
AuthCacheNode*
auth_cache_find(AuthCache*, Str*);
