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
	PartLockMgr lock_mgr_part;
	Catalog     catalog;
	WalMgr      wal_mgr;
	SnapshotMgr snapshot_mgr;
};

void db_init(Db*, CatalogIf*, void*, EngineIf*, void*);
void db_free(Db*);
void db_open(Db*, bool);
void db_close(Db*);
Buf* db_state(Db*);
void db_lock(Db*, PartLock*, uint64_t);
void db_unlock(Db*, PartLock*);
