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
	ServiceQueue   queue;
	List           workers;
	int            workers_count;
	Catalog*       catalog;
	WalMgr*        wal_mgr;
};

void service_init(Service*, Catalog*, WalMgr*);
void service_free(Service*);
void service_start(Service*);
void service_stop(Service*);

static inline void
service_lock(Service*     self,
             ServiceLock* lock,
             LockId       lock_type,
             uint64_t     id)
{
	service_lock_mgr_lock(&self->lock_mgr, lock, lock_type, id);
}

static inline void
service_unlock(ServiceLock* lock)
{
	service_lock_mgr_unlock(lock);
}

hot static inline void
service_add(Service* self, Table* table)
{
	if (self->workers_count > 0 && tier_mgr_created(&table->tier_mgr))
		service_queue_add(&self->queue, &table->config->id);
}
