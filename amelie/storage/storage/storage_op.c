
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

#include <amelie_runtime>
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
storage_checkpoint(Storage* self)
{
	// create new checkpoint
	// checkpoint(&self->checkpoints, &self->catalog);

	// run storage cleanup
	storage_gc(self);
}

void
storage_gc(Storage* self)
{
	// remove wal files < lsn
	auto lsn = state_checkpoint();
	wal_gc(&self->wal, lsn);

	// checkpoint gc
	// checkpoints_gc(&self->checkpoints);
}

static void
storage_sync_job(intptr_t* argv)
{
	auto storage = (Storage*)argv[0];
	auto id      = argv[1];
	auto close   = argv[2];
	auto file = wal_find(&storage->wal, id, false);
	if (! file)
		return;
	defer(wal_file_unpin_defer, file);
	wal_file_sync(file);
	if (close)
		wal_file_close(file);
}

void
storage_sync(Storage* self, uint64_t id, bool close)
{
	run(storage_sync_job, 3, self, id, close);
}

hot void
storage_write(Storage* self, WriteList* write_list)
{
	if (! write_list->list_count)
		return;

	WalContext context =
	{
		.list       = write_list,
		.recover    = NULL,
		.lsn        = 0,
		.sync_close = 0,
		.sync       = 0,
		.checkpoint = false
	};
	wal_write(&self->wal, &context);

	// todo:
#if 0
	// schedule sync and checkpoint service
	auto service = &self->service;
	if (unlikely(context.sync_close))
		service_schedule(service, ACTION_SYNC_CLOSE, context.sync_close);

	if (unlikely(context.sync))
		service_schedule(service, ACTION_SYNC, context.sync);

	if (unlikely(context.checkpoint))
		service_schedule(service, ACTION_CHECKPOINT, 0);
#endif
}

void
storage_write_recover(Storage* self, Buf* recover, uint64_t lsn)
{
	WalContext context =
	{
		.list       = NULL,
		.recover    = recover,
		.lsn        = lsn,
		.sync_close = 0,
		.sync       = 0,
		.checkpoint = false
	};
	wal_write(&self->wal, &context);

	// todo:
		// schedule sync and checkpoint service
}
