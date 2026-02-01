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

typedef struct StorageMgr StorageMgr;

struct StorageMgr
{
	RelationMgr mgr;
};

void storage_mgr_init(StorageMgr*);
void storage_mgr_free(StorageMgr*);
bool storage_mgr_create(StorageMgr*, Tr*, StorageConfig*, bool);
bool storage_mgr_drop(StorageMgr*, Tr*, Str*, bool);
bool storage_mgr_rename(StorageMgr*, Tr*, Str*, Str*, bool);
void storage_mgr_dump(StorageMgr*, Buf*);
Buf* storage_mgr_list(StorageMgr*, Str*, bool);
Storage*
storage_mgr_find(StorageMgr*, Str*, bool);
