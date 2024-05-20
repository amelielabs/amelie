#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct HandleMgr HandleMgr;

struct HandleMgr
{
	List list;
	int  list_count;
};

void    handle_mgr_init(HandleMgr*);
void    handle_mgr_free(HandleMgr*);
Handle* handle_mgr_get(HandleMgr*, Str*, Str*);
Handle* handle_mgr_get_by_id(HandleMgr*, Uuid*);
void    handle_mgr_create(HandleMgr*, Transaction*, LogCmd, Handle*, Buf*);
void    handle_mgr_drop(HandleMgr*, Transaction*, LogCmd, Handle*, Buf*);
void    handle_mgr_alter(HandleMgr*, Transaction*, LogCmd, Handle*, Buf*,
                         LogAbort, void*);
