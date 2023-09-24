#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct MetaCache MetaCache;

struct MetaCache
{
	HandleCache cache;
};

void  meta_cache_init(MetaCache*);
void  meta_cache_free(MetaCache*);
void  meta_cache_sync(MetaCache*, MetaCache*);
void  meta_cache_dump(MetaCache*, Buf*);
Meta* meta_cache_find(MetaCache*, Str*, bool);
Buf*  meta_cache_list(MetaCache*);
