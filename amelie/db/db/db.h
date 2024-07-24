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
	ViewMgr       view_mgr;
	TableMgr      table_mgr;
	CheckpointMgr checkpoint_mgr;
	Wal           wal;
};

void db_init(Db*, PartMapper, void*);
void db_free(Db*);
void db_open(Db*, CatalogMgr*);
void db_close(Db*);
