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

typedef struct CheckpointRef CheckpointRef;
typedef struct CheckpointMgr CheckpointMgr;

struct CheckpointRef
{
	uint64_t id;
	int      refs;
	List     link;
};

struct CheckpointMgr
{
	Spinlock       lock;
	CheckpointRef* current;
	List           list;
	int            list_count;
	Catalog*       catalog;
};

void checkpoint_mgr_init(CheckpointMgr*, Catalog*);
void checkpoint_mgr_free(CheckpointMgr*);
void checkpoint_mgr_open(CheckpointMgr*);
void checkpoint_mgr_gc(CheckpointMgr*);
void checkpoint_mgr_add(CheckpointMgr*, uint64_t);
CheckpointRef*
checkpoint_mgr_ref(CheckpointMgr*);
void checkpoint_mgr_unref(CheckpointMgr*, CheckpointRef*);
