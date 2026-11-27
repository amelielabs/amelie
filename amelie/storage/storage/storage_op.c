
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
#include <amelie_server>
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_service.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
storage_checkpoint(Storage* self)
{
	uint64_t min = UINT64_MAX;
	list_foreach(&self->catalog.rels.list)
	{
		auto db = db_of(list_at(Rel, link));
		uint64_t checkpoint = UINT64_MAX;
		service_checkpoint(&self->service, db->rel.id, &checkpoint);
		if (checkpoint < min)
			min = checkpoint;
	}
	if (min == UINT64_MAX)
		return;

	// todo: requires global checkpoint lock

	// remove database directories
	int removed = 0;
	for (;;)
	{
		auto gc = self->catalog.gc;
		if (! gc)
			break;
		self->catalog.gc = gc->next;
		Uuid id = gc->config->id;
		db_cleanup(gc);
		error_catch (
			checkpoints_cleanup(&id);
		);
		removed++;
	}

	// create new system catalog file
	storage_checkpoint_catalog(self);

	// report
	auto count = self->catalog.rels.list_count;
	if (removed > 0)
		info("checkpoint: {d} databases {d} removed", count, removed);
	else
		info("checkpoint: {d} databases", count);

	// remove older wal files
	wal_gc(&self->wal, min);
}

void
storage_checkpoint_db(Storage* self, Db* db)
{
	unused(self);
	if (! db->dirty)
		return;

	// prepare schema
	auto schema = buf_create();
	defer_buf(schema);
	describe_catalog(&db->catalog, schema);

	// create new checkpoint
	Checkpoint cp;
	checkpoint_init(&cp);
	defer(checkpoint_free, &cp);
	checkpoint_prepare(&cp, &db->page_mgr, &db->config->id, schema);
	if (checkpoint(&cp))
		checkpoints_add(&db->checkpoints, cp.lsn);

	db_dirty(db, false);
}

void
storage_checkpoint_catalog(Storage* self)
{
	auto lsn = state_lsn();
	if (lsn == self->catalog_lsn)
		return;

	// prepare databases schema
	auto schema = buf_create();
	defer_buf(schema);
	describe_databases(&self->catalog, schema);

	// write catalog file
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/storage/{u64}.incomplete",
	       state_directory(), lsn);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_WRONLY, 0644);
	if (! buf_empty(schema))
		file_write_buf(&file, schema);
	if (config()->storage_sync)
		file_sync(&file);

	// rename as completed
	fs_rename(path, "{s}/storage/{u64}", state_directory(), lsn);

	// sync storage directory
	if (config()->storage_sync)
		fs_syncdir("{s}/storage", state_directory());

	auto catalog_lsn_prev = self->catalog_lsn;
	self->catalog_lsn = lsn;

	// remove previous catalog file
	if (catalog_lsn_prev > 0)
		fs_unlink("{s}/storage/{u64}", state_directory(),
		          catalog_lsn_prev);
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
storage_write(Storage* self, Writes* list)
{
	if (! list->list_count)
		return;

	WalContext context =
	{
		.list       = list,
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
