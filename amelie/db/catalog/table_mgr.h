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
	StorageMgr* storage_mgr;
	EngineIf*   iface;
	void*       iface_arg;
};

void   table_mgr_init(TableMgr*, StorageMgr*, EngineIf*, void*);
void   table_mgr_free(TableMgr*);
bool   table_mgr_create(TableMgr*, Tr*, TableConfig*, bool);
bool   table_mgr_drop(TableMgr*, Tr*, Str*, Str*, bool);
void   table_mgr_drop_of(TableMgr*, Tr*, Table*);
bool   table_mgr_truncate(TableMgr*, Tr*, Str*, Str*, bool);
void   table_mgr_dump(TableMgr*, Buf*);
Table* table_mgr_find(TableMgr*, Str*, Str*, bool);
Table* table_mgr_find_by(TableMgr*, Uuid*, bool);
Buf*   table_mgr_list(TableMgr*, Str*, Str*, int);
