#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct TableMgr TableMgr;

struct TableMgr
{
	HandleMgr mgr;
};

void   table_mgr_init(TableMgr*);
void   table_mgr_free(TableMgr*);
void   table_mgr_create(TableMgr*, Transaction*, TableConfig*, bool);
void   table_mgr_drop(TableMgr*, Transaction*, Str*, Str*, bool);
void   table_mgr_drop_of(TableMgr*, Transaction*, Table*);
void   table_mgr_rename(TableMgr*, Transaction*, Str*, Str*, Str*, Str*, bool);
void   table_mgr_dump(TableMgr*, Buf*);
Table* table_mgr_find(TableMgr*, Str*, Str*, bool);
Table* table_mgr_find_by_id(TableMgr*, Uuid*);
Buf*   table_mgr_list(TableMgr*);
