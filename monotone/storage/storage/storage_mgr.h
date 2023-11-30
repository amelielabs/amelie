#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct StorageMgr StorageMgr;

struct StorageMgr
{
	List     list;
	int      list_count;
	Mapping* map;
};

void storage_mgr_init(StorageMgr*);
void storage_mgr_free(StorageMgr*);
void storage_mgr_open(StorageMgr*, Mapping*, Uuid*, List*, List*);
Storage*
storage_mgr_find(StorageMgr*, Uuid*);
