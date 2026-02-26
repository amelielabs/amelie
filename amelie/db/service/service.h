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

typedef struct Service Service;

struct Service
{
	ServiceLockMgr lock_mgr;
	Catalog*       catalog;
	WalMgr*        wal_mgr;
};

void service_init(Service*, Catalog*, WalMgr*);
void service_free(Service*);

hot static inline void
service_lock(Service* self, ServiceLock* lock, uint64_t id)
{
	service_lock_mgr_lock(&self->lock_mgr, lock, id);
}

hot static inline void
service_unlock(ServiceLock* lock)
{
	service_lock_mgr_unlock(lock);
}
