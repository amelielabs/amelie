
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
storage_open(Storage* self, Db* db)
{
	unused(self);

	// open last checkpoint
	char uuid[UUID_SZ];
	char path[PATH_MAX];
	uuid_get(&db->config->id, uuid, sizeof(uuid));
	format(path, sizeof(path), "{s}/storage/{s}/{u64}",
	       state_directory(), uuid,
	       db->checkpoints.current->id);

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
storage_checkpoint(Storage* self, Db* db)
{
	unused(self);

	// prepare db schema
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
