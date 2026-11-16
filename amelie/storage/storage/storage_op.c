
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

static bool
storage_open_schema(Storage* self, Db* db)
{
	// read database directory
	auto cp = &db->checkpoints;
	checkpoints_open(cp);
	if (! cp->current)
		return false;

	// restore last db checkpoint schema
	char uuid[UUID_SZ];
	uuid_get(&db->config->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/storage/{s}/{u64}.sql",
	       state_directory(), uuid, cp->current->id);

	// read file
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);
	file_import_stream(&sep.buf, "{s}", path);

	// prepare eval
	Eval eval;
	eval_init(&eval, self->iface, self->iface_arg);
	eval_create(&eval);
	defer(eval_free, &eval);

	// parse and execute statements
	Str command;
	while (separator_read(&sep, &command))
	{
		eval_execute(&eval, &db->config->name, &command);
		separator_advance(&sep);
	}
	return true;
}

void
storage_open_db(Db* db)
{
	// open last checkpoint
	char uuid[UUID_SZ];
	char path[PATH_MAX];
	uuid_get(&db->config->id, uuid, sizeof(uuid));
	format(path, sizeof(path), "{s}/storage/{s}/{u64}", state_directory(),
	       uuid, db->checkpoints.current->id);

	// read pages
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_RDONLY, 0);

	uint64_t offset = 0;
	while (offset < file.size)
	{
		auto page = page_mgr_pop(&db->page_mgr);
		offset = page_load(page, &file, offset);

		auto rel   = catalog_find_by(&db->catalog, REL_TABLE, &page->id_rel, true);
		auto table = table_of(rel);
		auto part  = table->part;

		switch (page->type) {
		case PAGE_HEAP_META:
		case PAGE_HEAP:
		{
			pages_import(&part->heap->pages, page);
			break;
		}
		case PAGE_FLAT_META:
		case PAGE_FLAT:
		{
			auto column = columns_find_by(&table->config->columns, page->id_column);
			auto flat = flats_at(&part->flats, column);
			pages_import(&flat->pages, page);
			break;
		}
		default:
			break;
		}
	}

	// complete
	list_foreach(&db->catalog.rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_TABLE)
			continue;

		// finilize import
		auto table = table_of(rel);
		auto part  = table->part;
		pages_import_done(&part->heap->pages);
		list_foreach(&table->config->columns.list)
		{
			auto column = list_at(Column, link);
			if (! column->size_flat)
				continue;
			auto flat = flats_at(&part->flats, column);
			pages_import_done(&flat->pages);
		}

		// build indexes
		part_open(part);
	}
}

void
storage_open(Storage* self, Db* db)
{
	if (storage_open_schema(self, db))
		storage_open_db(db);
}

void
storage_checkpoint(Storage* self, Db* db)
{
	unused(self);

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
		; // add
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
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	// rename as completed
	fs_rename(path, "{s}/storage/{u64}", state_directory(), lsn);

	// sync storage directory
	if (opt_int_of(&config()->storage_sync))
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
