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
	WalMgr      wal_mgr;
	Service     service;
	SnapshotMgr snapshot_mgr;
};

void      db_init(Db*, CatalogIf*, void*, PartMgrIf*, void*);
void      db_free(Db*);
void      db_open(Db*, bool);
void      db_close(Db*);
Snapshot* db_snapshot(Db*);
void      db_snapshot_drop(Db*, Snapshot*);
Buf*      db_state(Db*);
