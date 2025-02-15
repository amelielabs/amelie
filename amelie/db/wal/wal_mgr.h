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

typedef struct WalMgr WalMgr;

struct WalMgr
{
	Wal       wal;
	WalWorker wal_worker;
};

void wal_mgr_init(WalMgr*);
void wal_mgr_free(WalMgr*);
void wal_mgr_start(WalMgr*);
void wal_mgr_stop(WalMgr*);
void wal_mgr_gc(WalMgr*);
void wal_mgr_rotate(WalMgr*);
void wal_mgr_write(WalMgr*, WalBatch*);
