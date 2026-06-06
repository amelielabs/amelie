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

typedef struct CheckpointMgr CheckpointMgr;

struct CheckpointMgr
{
	Checkpoint* last;
	List        list;
	int         list_count;
};

void checkpoint_mgr_init(CheckpointMgr*);
void checkpoint_mgr_free(CheckpointMgr*);
void checkpoint_mgr_open(CheckpointMgr*);
void checkpoint_mgr_gc(CheckpointMgr*);
void checkpoint_mgr_add(CheckpointMgr*, Checkpoint*);
