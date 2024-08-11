#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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

void checkpoint_mgr_init(CheckpointMgr*, CheckpointIf*, void*);
void checkpoint_mgr_free(CheckpointMgr*);
void checkpoint_mgr_open(CheckpointMgr*);
void checkpoint_mgr_gc(CheckpointMgr*);
void checkpoint_mgr_add(CheckpointMgr*, uint64_t);
