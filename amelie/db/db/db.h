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
	PartMgr       part_mgr;
	ProcMgr       proc_mgr;
	WalMgr        wal_mgr;
	Checkpointer  checkpointer;
	CheckpointMgr checkpoint_mgr;
};

void db_init(Db*, PartAttach, void*, ProcIf*, void*);
void db_free(Db*);
void db_open(Db*);
void db_close(Db*);
Buf* db_status(Db*);
Buf* db_state(Db*);
