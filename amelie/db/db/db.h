#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Db Db;

struct Db
{
	SchemaMgr     schema_mgr;
	TableMgr      table_mgr;
	ViewMgr       view_mgr;
	Checkpointer  checkpointer;
	CheckpointMgr checkpoint_mgr;
	Wal           wal;
};

void db_init(Db*, PartMapper, void*);
void db_free(Db*);
void db_open(Db*);
void db_close(Db*);
