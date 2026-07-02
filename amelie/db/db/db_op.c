
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>
#include <amelie_db.h>

void
db_gc(Db* self)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	auto lsn = state_checkpoint();

	// get min cdc slot lsn
	uint64_t cdc_lsn;
	auto     cdc = self->cdc;
	cdc_min(cdc, &cdc_lsn);
	if (cdc_lsn < lsn)
		lsn = cdc_lsn;

	unlock(lock_catalog);

	// remove wal files < lsn
	wal_gc(&self->wal, lsn);

	// cdc gc
	cdc_gc(cdc);

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

void
db_checkpoint(Db* self)
{
	uint64_t lsn = state_lsn();
	if (lsn == state_checkpoint())
		return;

	// one checkpoint (or create index) at a time
	auto checkpoint_lock = lock_system(REL_CHECKPOINT, LOCK_EXCLUSIVE);
	defer(unlock, checkpoint_lock);

	// take exclusive catalog lock
	auto catalog_lock = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);

	// force commit pending prepared transactions
	list_foreach(&self->catalog.rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_TABLE)
			continue;
		auto table = table_of(rel);
		list_foreach(&table->parts.list)
		{
			auto part = list_at(Part, link);
			auto consensus = &part->track.consensus;
			track_sync(&part->track, consensus);
		}
	}

	// prepare and start workers
	Checkpoint checkpoint;
	checkpoint_init(&checkpoint, &self->catalog);
	defer(checkpoint_free, &checkpoint);
	auto on_error = error_catch
	(
		checkpoint_begin(&checkpoint, lsn, 1);
		checkpoint_run(&checkpoint);
	);

	// unlock catalog
	unlock(catalog_lock);

	if (on_error)
		rethrow();

	// wait for completion
	on_error = error_catch (
		checkpoint_wait(&checkpoint);
	);
	if (on_error)
		rethrow();

	// set checkpoint
	checkpoints_add(&self->checkpoints, lsn);

	// run db cleanup
	db_gc(self);
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
