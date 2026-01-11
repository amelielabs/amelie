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

typedef struct Storage Storage;

struct Storage
{
	LockMgr       lock_mgr;
	Catalog       catalog;
	Service       service;
	ServiceMgr    service_mgr;
	Deploy        deploy;
	CompactionMgr compaction_mgr;
	WalMgr        wal_mgr;
};

void storage_init(Storage*, CatalogIf*, void*, DeployIf*, void*);
void storage_free(Storage*);
void storage_open(Storage*);
void storage_close(Storage*);
Buf* storage_state(Storage*);
void storage_lock(Storage*, ServiceLock*, Id*);
void storage_unlock(Storage*, ServiceLock*);
