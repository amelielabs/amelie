#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TableMgr TableMgr;

struct TableMgr
{
	HandleMgr   tables;
	CompactMgr* compact_mgr;
};

void   table_mgr_init(TableMgr*, CompactMgr*);
void   table_mgr_free(TableMgr*);
void   table_mgr_gc(TableMgr*, uint64_t);
void   table_mgr_create(TableMgr*, Transaction*, TableConfig*, bool);
void   table_mgr_drop(TableMgr*, Transaction*, Str*, bool);
void   table_mgr_alter(TableMgr*, Transaction*, Str*, TableConfig*, bool);
Table* table_mgr_find(TableMgr*, Str*, bool);
Table* table_mgr_find_by_id(TableMgr*, Uuid*);
Buf*   table_mgr_show(TableMgr*);
