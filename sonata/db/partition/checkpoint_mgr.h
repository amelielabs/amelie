#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct CheckpointMgr CheckpointMgr;

struct CheckpointMgr
{
	IdMgr list_snapshot;
	IdMgr list;
};

void checkpoint_mgr_init(CheckpointMgr*);
void checkpoint_mgr_free(CheckpointMgr*);
void checkpoint_mgr_open(CheckpointMgr*, CatalogMgr*);
void checkpoint_mgr_gc(CheckpointMgr*);
void checkpoint_mgr_add(CheckpointMgr*, uint64_t);
