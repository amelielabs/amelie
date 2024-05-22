#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Db Db;

struct Db
{
	SchemaMgr schema_mgr;
	ViewMgr   view_mgr;
	TableMgr  table_mgr;
	Wal       wal;
};

void db_init(Db*);
void db_free(Db*);
void db_open(Db*, CatalogMgr*);
void db_close(Db*);
