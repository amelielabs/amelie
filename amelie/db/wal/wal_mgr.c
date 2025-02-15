
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
	if (var_int_of(&config()->wal_worker))
		wal_worker_start(&self->wal_worker);
}

void
wal_mgr_stop(WalMgr* self)
{
	// stop wal worker
	if (var_int_of(&config()->wal_worker))
	{
		int flags = WAL_SHUTDOWN;
		if (var_int_of(&config()->wal_sync_on_shutdown))
			flags |= WAL_SYNC;
		wal_worker_schedule(&self->wal_worker, flags);
		wal_worker_stop(&self->wal_worker);
	}

	// shutdown wal mgr
	wal_close(&self->wal);
}

void
wal_mgr_gc(WalMgr* self)
{
	wal_gc(&self->wal, state_checkpoint());
}

void
wal_mgr_rotate(WalMgr* self)
{
	if (var_int_of(&config()->wal_worker))
	{
		int flags = WAL_ROTATE;
		if (var_int_of(&config()->wal_sync_on_rotate))
			flags |= WAL_SYNC;
		wal_worker_schedule(&self->wal_worker, flags);
	} else
	{
		wal_rotate(&self->wal, state_lsn() + 1);
	}
}

void
wal_mgr_write(WalMgr* self, WalBatch* batch)
{
	auto wal_rotate = wal_write(&self->wal, batch);
	if (var_int_of(&config()->wal_sync_on_write))
		wal_sync(&self->wal);
	if (wal_rotate)
		wal_mgr_rotate(self);
}
