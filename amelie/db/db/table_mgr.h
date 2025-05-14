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

typedef struct TableMgr TableMgr;

struct TableMgr
{
	RelationMgr mgr;
	PartMgr*    part_mgr;
};

void   table_mgr_init(TableMgr*, PartMgr*);
void   table_mgr_free(TableMgr*);
bool   table_mgr_create(TableMgr*, Tr*, TableConfig*, bool);
void   table_mgr_drop(TableMgr*, Tr*, Str*, Str*, bool);
void   table_mgr_drop_of(TableMgr*, Tr*, Table*);
void   table_mgr_truncate(TableMgr*, Tr*, Str*, Str*, bool);
void   table_mgr_dump(TableMgr*, Buf*);
Table* table_mgr_find(TableMgr*, Str*, Str*, bool);
Buf*   table_mgr_list(TableMgr*, Str*, Str*, bool);
