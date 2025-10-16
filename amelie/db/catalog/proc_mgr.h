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

typedef struct ProcMgr ProcMgr;

struct ProcMgr
{
	RelationMgr mgr;
	ProcFree    free;
	void*       free_arg;
};

void  proc_mgr_init(ProcMgr*, ProcFree, void*);
void  proc_mgr_free(ProcMgr*);
bool  proc_mgr_create(ProcMgr*, Tr*, ProcConfig*, bool);
bool  proc_mgr_drop(ProcMgr*, Tr*, Str*, Str*, bool);
void  proc_mgr_drop_of(ProcMgr*, Tr*, Proc*);
bool  proc_mgr_rename(ProcMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void  proc_mgr_dump(ProcMgr*, Buf*);
Buf*  proc_mgr_list(ProcMgr*, Str*, Str*, bool);
Proc* proc_mgr_find(ProcMgr*, Str*, Str*, bool);
