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
void storage_mgr_list(StorageMgr*, Buf*);
void storage_mgr_snapshot(StorageMgr*, SnapshotWriter*, Snapshot*, uint64_t);

Storage*
storage_mgr_find(StorageMgr*, Uuid*);

Storage*
storage_mgr_find_by_shard(StorageMgr*, Uuid*);
