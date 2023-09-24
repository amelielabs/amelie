#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct StorageMgr StorageMgr;

struct StorageMgr
{
	List       list;
	int        list_count;
	CompactMgr compact_mgr;
};

void storage_mgr_init(StorageMgr*);
void storage_mgr_free(StorageMgr*);
void storage_mgr_open(StorageMgr*);
void storage_mgr_gc(StorageMgr*);

Storage*
storage_mgr_create(StorageMgr*, StorageConfig*);

Storage*
storage_mgr_find(StorageMgr*, Uuid*);

Buf*
storage_mgr_show(StorageMgr*);
