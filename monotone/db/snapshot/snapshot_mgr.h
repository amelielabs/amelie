#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Snapshot    Snapshot;
typedef struct SnapshotMgr SnapshotMgr;

struct Snapshot
{
	uint64_t  id;
	Uuid      uuid;
	Snapshot* uuid_next;
	List      link;
};

struct SnapshotMgr
{
	Mutex lock;
	IdMgr refs;
	List  list;
};

void snapshot_mgr_init(SnapshotMgr*);
void snapshot_mgr_free(SnapshotMgr*);
void snapshot_mgr_open(SnapshotMgr*);
void snapshot_mgr_add(SnapshotMgr*, Uuid*, uint64_t);
void snapshot_mgr_gc(SnapshotMgr*, uint64_t);
