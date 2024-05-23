#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct StorageMgr StorageMgr;

struct StorageMgr
{
	bool reference;
	List list;
	int  list_count;
};

void storage_mgr_init(StorageMgr*);
void storage_mgr_free(StorageMgr*);
void storage_mgr_open(StorageMgr*, bool, List*, List*);

Storage*
storage_mgr_find(StorageMgr*, uint64_t);

Storage*
storage_mgr_match(StorageMgr*, Uuid*);
