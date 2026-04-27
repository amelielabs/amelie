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
	RelMgr   mgr;
	Columns  columns;
	Catalog* catalog;
	Cdc*     cdc;
};

void sub_mgr_init(SubMgr*, Catalog*, Cdc*);
void sub_mgr_free(SubMgr*);
void sub_mgr_open(SubMgr*);
bool sub_mgr_create(SubMgr*, Tr*, SubConfig*, bool);
bool sub_mgr_drop(SubMgr*, Tr*, Str*, Str*, bool);
void sub_mgr_drop_of(SubMgr*, Tr*, Sub*);
bool sub_mgr_rename(SubMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
bool sub_mgr_grant(SubMgr*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
void sub_mgr_dump(SubMgr*, Buf*);
void sub_mgr_list(SubMgr*, Buf*, Str*, Str*, int);
Sub* sub_mgr_find(SubMgr*, Str*, Str*, bool);
Sub* sub_mgr_find_by(SubMgr*, Uuid*, bool);
