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
	PartMgr       part_mgr;
	WalMgr        wal_mgr;
	CheckpointMgr checkpoint_mgr;
	Checkpointer  checkpointer;
};

void db_init(Db*, CatalogIf*, void*, PartAttach, void*);
void db_free(Db*);
void db_open(Db*);
void db_close(Db*);
Buf* db_state(Db*);
