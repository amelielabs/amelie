#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct TableMgr TableMgr;

struct TableMgr
{
	HandleMgr mgr;
	PartMgr   part_mgr;
};

void   table_mgr_init(TableMgr*, PartMapper, void*);
void   table_mgr_free(TableMgr*);
bool   table_mgr_create(TableMgr*, Transaction*, TableConfig*, bool);
void   table_mgr_drop(TableMgr*, Transaction*, Str*, Str*, bool);
void   table_mgr_drop_of(TableMgr*, Transaction*, Table*);
void   table_mgr_rename(TableMgr*, Transaction*, Str*, Str*, Str*, Str*, bool);
void   table_mgr_dump(TableMgr*, Buf*);
Table* table_mgr_find(TableMgr*, Str*, Str*, bool);
Buf*   table_mgr_list(TableMgr*);
Part*  table_mgr_find_partition(TableMgr*, uint64_t);
