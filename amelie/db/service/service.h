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

static inline void
service_init(Service* self, Catalog* catalog, WalMgr* wal_mgr)
{
	self->catalog = catalog;
	self->wal_mgr = wal_mgr;
	service_lock_mgr_init(&self->lock_mgr);
}

static inline void
service_free(Service* self)
{
	service_lock_mgr_free(&self->lock_mgr);
}

hot static inline void
service_lock(Service*     self,
             ServiceLock* lock,
             LockId       lock_type,
             uint64_t     id)
{
	service_lock_mgr_lock(&self->lock_mgr, lock, lock_type, id);
}

hot static inline void
service_unlock(ServiceLock* lock)
{
	service_lock_mgr_unlock(lock);
}
