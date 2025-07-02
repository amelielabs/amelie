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

typedef struct DbIf DbIf;
typedef struct Db   Db;

struct DbIf
{
	void (*build_index)(Db*, Table*, IndexConfig*);
	void (*build_column_add)(Db*, Table*, Table*, Column*);
	void (*build_column_drop)(Db*, Table*, Table*, Column*);
};

struct Db
{
	SchemaMgr     schema_mgr;
	TableMgr      table_mgr;
	PartMgr       part_mgr;
	WalMgr        wal_mgr;
	Checkpointer  checkpointer;
	CheckpointMgr checkpoint_mgr;
	DbIf*         iface;
	void*         iface_arg;
};

void db_init(Db*, DbIf*, void*, PartAttach, void*);
void db_free(Db*);
void db_open(Db*);
void db_close(Db*);
Buf* db_status(Db*);
Buf* db_state(Db*);
