#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct WalStore WalStore;

struct WalStore
{
	SnapshotMgr snapshot;
	SnapshotMgr list;
	WalFile*    current;
	WalRecord   record;
	WalEventMgr event_mgr;
};

void wal_store_init(WalStore*);
void wal_store_free(WalStore*);
void wal_store_rotate(WalStore*, uint64_t);
void wal_store_gc(WalStore*, uint64_t);
void wal_store_snapshot(WalStore*, WalSnapshot*);
void wal_store_write(WalStore*, Log*);
Buf* wal_store_status(WalStore*);
