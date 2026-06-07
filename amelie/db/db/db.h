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
	Catalog       catalog;
	Wal           wal;
	List          snapshots;
	int           snapshots_count;
	CheckpointMgr checkpoint_mgr;
	Syncer        syncer;
	Cdc*          cdc;
};

void db_init(Db*, CatalogIf*, void*, PartMgrIf*, void*, Cdc*);
void db_free(Db*);
void db_open(Db*, bool);
void db_close(Db*);
void db_state(Db*, Buf*);
