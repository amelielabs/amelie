
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>

void
wal_mgr_init(WalMgr* self)
{
	wal_init(&self->wal);
	wal_worker_init(&self->wal_worker, &self->wal);
	wal_periodic_init(&self->wal_periodic, &self->wal_worker);
}

void
wal_mgr_free(WalMgr* self)
{
	wal_worker_free(&self->wal_worker);
	wal_free(&self->wal);
}

void
wal_mgr_start(WalMgr* self)
{
	// open wal directory
	wal_open(&self->wal);

	// start wal worker
	if (opt_int_of(&config()->wal_worker))
		wal_worker_start(&self->wal_worker);
}

void
wal_mgr_stop(WalMgr* self)
{
	// stop periodic sync
	wal_periodic_stop(&self->wal_periodic);

	// stop wal worker
	if (opt_int_of(&config()->wal_worker))
	{
		int flags = WAL_SHUTDOWN;
		if (opt_int_of(&config()->wal_sync_on_close))
			flags |= WAL_SYNC;
		wal_worker_request(&self->wal_worker, flags);
		wal_worker_stop(&self->wal_worker);
	} else
	{
		if (opt_int_of(&config()->wal_sync_on_close))
			wal_sync(&self->wal, false);
	}

	// shutdown wal
	wal_close(&self->wal);
}

void
wal_mgr_gc(WalMgr* self)
{
	wal_gc(&self->wal, state_checkpoint());
}

void
wal_mgr_create(WalMgr* self)
{
	if (opt_int_of(&config()->wal_worker))
	{
		int flags = WAL_CREATE;
		if (opt_int_of(&config()->wal_sync_on_close))
			flags |= WAL_SYNC;
		if (opt_int_of(&config()->wal_sync_on_create))
			flags |= WAL_SYNC_CREATE;
		wal_worker_request(&self->wal_worker, flags);
	} else
	{
		if (opt_int_of(&config()->wal_sync_on_close))
			wal_sync(&self->wal, false);
		wal_create(&self->wal, state_lsn() + 1);
		if (opt_int_of(&config()->wal_sync_on_create))
			wal_sync(&self->wal, true);
	}
}

void
wal_mgr_write(WalMgr* self, WriteList* write_list)
{
	if (! write_list->list_count)
		return;
	auto wal_rotate = wal_write(&self->wal, write_list);
	if (opt_int_of(&config()->wal_sync_on_write))
		wal_sync(&self->wal, false);
	if (wal_rotate)
		wal_mgr_create(self);
}
