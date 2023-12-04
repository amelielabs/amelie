#pragma once

//
// monotone
//
// SQL OLTP database
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
