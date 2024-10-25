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

typedef struct HandleMgr HandleMgr;

struct HandleMgr
{
	List list;
	int  list_count;
};

void    handle_mgr_init(HandleMgr*);
void    handle_mgr_free(HandleMgr*);
Handle* handle_mgr_get(HandleMgr*, Str*, Str*);
void    handle_mgr_replace(HandleMgr*, Handle*, Handle*);
void    handle_mgr_create(HandleMgr*, Tr*, LogCmd, Handle*, Buf*);
void    handle_mgr_drop(HandleMgr*, Tr*, LogCmd, Handle*, Buf*);
