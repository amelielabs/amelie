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
typedef struct CheckpointIf  CheckpointIf;
typedef struct Checkpoint    Checkpoint;

struct CheckpointIf
{
	Buf* (*catalog_dump)(void*);
	void (*catalog_restore)(uint8_t**, void*);
	void (*add)(Checkpoint*, void*);
	void (*complete)(void*);
};

struct CheckpointMgr
{
	IdMgr         list_snapshot;
	IdMgr         list;
	CheckpointIf* iface;
	void*         iface_arg;
};

void     checkpoint_mgr_init(CheckpointMgr*, CheckpointIf*, void*);
void     checkpoint_mgr_free(CheckpointMgr*);
void     checkpoint_mgr_open(CheckpointMgr*);
void     checkpoint_mgr_gc(CheckpointMgr*);
void     checkpoint_mgr_add(CheckpointMgr*, uint64_t);
void     checkpoint_mgr_list(CheckpointMgr*, uint64_t, Buf*);
uint64_t checkpoint_mgr_snapshot(CheckpointMgr*);
