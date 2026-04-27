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

typedef struct BranchMgr BranchMgr;

struct BranchMgr
{
	RelMgr    mgr;
	TableMgr* table_mgr;
};

void    branch_mgr_init(BranchMgr*, TableMgr*);
void    branch_mgr_free(BranchMgr*);
bool    branch_mgr_create(BranchMgr*, Tr*, BranchConfig*, bool);
bool    branch_mgr_drop(BranchMgr*, Tr*, Str*, Str*, bool);
void    branch_mgr_drop_of(BranchMgr*, Tr*, Branch*);
bool    branch_mgr_rename(BranchMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
bool    branch_mgr_grant(BranchMgr*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
void    branch_mgr_dump(BranchMgr*, Buf*);
void    branch_mgr_list(BranchMgr*, Buf*, Str*, Str*, int);
Branch* branch_mgr_find(BranchMgr*, Str*, Str*, bool);
