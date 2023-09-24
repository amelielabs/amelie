#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HandleCache HandleCache;

struct HandleCache
{
	List list;
	int  list_count;
};

void    handle_cache_init(HandleCache*);
void    handle_cache_free(HandleCache*);
Handle* handle_cache_set(HandleCache*, Handle*);
Handle* handle_cache_delete(HandleCache*, Str*);
Handle* handle_cache_get(HandleCache*, Str*);
void    handle_cache_abort(HandleCache*, Handle*, Handle*);
void    handle_cache_commit(HandleCache*, Handle*, Handle*, uint64_t);
void    handle_cache_sync(HandleCache*, HandleCache*);
