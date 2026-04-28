
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

bool
table_mgr_create(Catalog*     self,
                 Tr*          tr,
                 TableConfig* config,
                 bool         if_not_exists)
{
	// make sure table does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// allocate table
	auto table = table_allocate(config, &self->storage_mgr,
	                            self->iface_part,
	                            self->iface_part_arg);

	// update tables
	rel_mgr_create(&self->rels, tr, &table->rel);

	// prepare partitions
	table_open(table);
	return true;
}

void
table_mgr_drop_of(Catalog* self, Tr* tr, Table* table)
{
	// drop table by object
	rel_mgr_drop(&self->rels, tr, &table->rel);
}

bool
table_mgr_drop(Catalog* self, Tr* tr, Str* user, Str* name,
               bool if_exists)
{
	auto table = catalog_find_table(self, user, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &table->rel);

	table_mgr_drop_of(self, tr, table);
	return true;
}

static void
truncate_if_commit(Log* self, LogOp* op)
{
	unused(self);
	// truncate partitions
	auto table = table_of(op->rel);
	part_mgr_truncate(&table->part_mgr);
}

static void
truncate_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf truncate_if =
{
	.commit = truncate_if_commit,
	.abort  = truncate_if_abort
};

bool
table_mgr_truncate(Catalog* self,
                   Tr*      tr,
                   Str*     user,
                   Str*     name,
                   bool     if_exists)
{
	auto table = catalog_find_table(self, user, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &table->rel);

	// update table
	log_ddl(&tr->log, &truncate_if, NULL, &table->rel);

	// do nothing (actual truncate will happen on commit)
	return true;
}
