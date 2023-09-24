#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TableCache TableCache;

struct TableCache
{
	HandleCache cache;
};

void   table_cache_init(TableCache*);
void   table_cache_free(TableCache*);
void   table_cache_sync(TableCache*, TableCache*);
void   table_cache_dump(TableCache*, Buf*);
Table* table_cache_find(TableCache*, Str*, bool);
Table* table_cache_find_by_id(TableCache*, Uuid*);
Buf*   table_cache_list(TableCache*);
