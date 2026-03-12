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

typedef struct Db Db;

struct Db
{
	Catalog     catalog;
	Wal         wal;
	Service     service;
	Syncer      syncer;
	SnapshotMgr snapshot_mgr;
};

void      db_init(Db*, CatalogIf*, void*, PartMgrIf*, void*);
void      db_free(Db*);
void      db_open(Db*, bool);
void      db_close(Db*);
Snapshot* db_snapshot(Db*);
void      db_snapshot_drop(Db*, Snapshot*);
void      db_write(Db*, WriteList*);
Buf*      db_state(Db*);
