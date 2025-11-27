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

typedef struct Storage Storage;

struct Storage
{
	Catalog       catalog;
	PartMgr       part_mgr;
	WalMgr        wal_mgr;
	CheckpointMgr checkpoint_mgr;
	Checkpointer  checkpointer;
};

void storage_init(Storage*, CatalogIf*, void*, PartAttach, void*);
void storage_free(Storage*);
void storage_open(Storage*);
void storage_close(Storage*);
Buf* storage_state(Storage*);
