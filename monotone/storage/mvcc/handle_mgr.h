#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HandleMgr HandleMgr;

struct HandleMgr
{
	List list;
	int  list_count;
};

void    handle_mgr_init(HandleMgr*);
void    handle_mgr_free(HandleMgr*);
Handle* handle_mgr_set(HandleMgr*, Handle*);
Handle* handle_mgr_delete(HandleMgr*, Str*);
Handle* handle_mgr_get(HandleMgr*, Str*);
void    handle_mgr_abort(HandleMgr*, Handle*, Handle*);
void    handle_mgr_commit(HandleMgr*, Handle*, Handle*, uint64_t);
