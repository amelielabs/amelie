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
	ProcIf*     iface;
	void*       iface_arg;
	RelationMgr mgr;
};

void  proc_mgr_init(ProcMgr*, ProcIf*, void*);
void  proc_mgr_free(ProcMgr*);
void  proc_mgr_create(ProcMgr*, Tr*, ProcConfig*, bool);
void  proc_mgr_drop(ProcMgr*, Tr*, Str*, Str*, bool);
void  proc_mgr_drop_of(ProcMgr*, Tr*, Proc*);
void  proc_mgr_rename(ProcMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void  proc_mgr_dump(ProcMgr*, Buf*);
Buf*  proc_mgr_list(ProcMgr*, Str*, Str*, bool);
Proc* proc_mgr_find(ProcMgr*, Str*, Str*, bool);
Proc* proc_mgr_find_dep(ProcMgr*, Str*, Str*);
