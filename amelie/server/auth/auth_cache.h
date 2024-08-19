#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AuthCacheNode AuthCacheNode;
typedef struct AuthCache     AuthCache;

struct AuthCacheNode
{
	User*         user;
	Str           digest;
	HashtableNode node;
};

struct AuthCache
{
	Hashtable ht;
};

void  auth_cache_init(AuthCache*);
void  auth_cache_free(AuthCache*);
void  auth_cache_reset(AuthCache*);
void  auth_cache_add(AuthCache*, User*, Str*);
User* auth_cache_find(AuthCache*, Str*);
