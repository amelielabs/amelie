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
	NodeMgr       node_mgr;
	Checkpointer  checkpointer;
	CheckpointMgr checkpoint_mgr;
	Wal           wal;
};

void db_init(Db*, PartMapper, void*, NodeIf*, void*);
void db_free(Db*);
void db_open(Db*);
void db_close(Db*, bool);
Buf* db_status(Db*);
