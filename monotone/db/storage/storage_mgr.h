#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct StorageMgr StorageMgr;

struct StorageMgr
{
	List list;
	int  list_count;
};

void storage_mgr_init(StorageMgr*);
void storage_mgr_free(StorageMgr*);
void storage_mgr_open(StorageMgr*);
void storage_mgr_gc(StorageMgr*);
Buf* storage_mgr_list(StorageMgr*);

Storage*
storage_mgr_find(StorageMgr*, Uuid*);
Storage*
storage_mgr_find_for(StorageMgr*, Uuid*, Uuid*);

Storage*
storage_mgr_create(StorageMgr*, StorageConfig*);
void storage_mgr_attach(StorageMgr*, Storage*, Index*);
void storage_mgr_detach(StorageMgr*, Storage*, Index*);
