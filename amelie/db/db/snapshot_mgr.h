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

typedef struct Snapshot    Snapshot;
typedef struct SnapshotMgr SnapshotMgr;

struct Snapshot
{
	Buf     data;
	WalSlot wal_snapshot;
	List    link;
};

struct SnapshotMgr
{
	List     list;
	List     list_gc;
	int      list_count;
	Catalog* catalog;
	Wal*     wal;
};

void      snapshot_mgr_init(SnapshotMgr*, Catalog*, Wal*);
void      snapshot_mgr_free(SnapshotMgr*);
Snapshot* snapshot_mgr_create(SnapshotMgr*);
void      snapshot_mgr_drop(SnapshotMgr*, Snapshot*);
