#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Db Db;

struct Db
{
	TableMgr   table_mgr;
	MetaMgr    meta_mgr;
	CompactMgr compact_mgr;
	Wal        wal;
};

void db_init(Db*, MetaIface*, void*);
void db_free(Db*);
void db_open(Db*, CatalogMgr*);
void db_recover(Db*);
void db_close(Db*);
void db_checkpoint(Db*);
