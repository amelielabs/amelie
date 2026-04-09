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

typedef struct SubMgr SubMgr;

struct SubMgr
{
	int     list_count;
	List    list;
	PubMgr* pub_mgr;
};

void sub_mgr_init(SubMgr*, PubMgr*);
void sub_mgr_free(SubMgr*);
void sub_mgr_open(SubMgr*);
void sub_mgr_create(SubMgr*, SubConfig*, bool);
void sub_mgr_drop(SubMgr*, Str*, Str*, bool);
Buf* sub_mgr_list(SubMgr*, Str*, Str*, int);
Sub* sub_mgr_find(SubMgr*, Str*, Str*, bool);
