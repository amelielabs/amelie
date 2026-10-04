
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
db_checkpoint(Db* self)
{
	// create new checkpoint
	checkpoint(&self->checkpoints, &self->catalog);

	// run db cleanup
	db_gc(self);
}

void
db_gc(Db* self)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	auto lsn = state_checkpoint();

	unlock(lock_catalog);

	// remove wal files < lsn
	wal_gc(&self->wal, lsn);

	// checkpoint gc
	checkpoints_gc(&self->checkpoints);
}

static void
db_sync_job(intptr_t* argv)
{
	auto db      = (Db*)argv[0];
	auto id      = argv[1];
	auto close   = argv[2];
	auto file = wal_find(&db->wal, id, false);
	if (! file)
		return;
	defer(wal_file_unpin_defer, file);
	wal_file_sync(file);
	if (close)
		wal_file_close(file);
}

void
db_sync(Db* self, uint64_t id, bool close)
{
	run(db_sync_job, 3, self, id, close);
}

hot void
db_write(Db* self, WriteList* write_list)
{
	if (! write_list->list_count)
		return;

	WalContext context =
	{
		.list       = write_list,
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
