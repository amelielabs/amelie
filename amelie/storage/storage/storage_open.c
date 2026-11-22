
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

static void
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
storage_open(Storage* self, Task* compute)
{
	// open all databases from this compute
	list_foreach(&self->catalog.rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_DB)
			continue;
		auto db = db_of(rel);
		if (db->compute != compute)
			continue;
		if (storage_open_schema(self, db))
			storage_open_db(db);
		db_set_dirty(db, false);
	}
}
