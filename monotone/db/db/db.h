#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Db Db;

struct Db
{
	MetaMgr    meta_mgr;
	TableMgr   table_mgr;
	StorageMgr storage_mgr;
	Wal        wal;
};

void db_init(Db*, MetaIf*, void*);
void db_free(Db*);
void db_open(Db*, CatalogMgr*);
void db_close(Db*);
