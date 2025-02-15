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
	SchemaMgr     schema_mgr;
	TableMgr      table_mgr;
	WorkerMgr     worker_mgr;
	Checkpointer  checkpointer;
	CheckpointMgr checkpoint_mgr;
	WalMgr        wal_mgr;
};

void db_init(Db*, PartMapper, void*, WorkerIf*, void*);
void db_free(Db*);
void db_open(Db*);
void db_close(Db*, bool);
Buf* db_status(Db*);
Buf* db_state(Db*);
