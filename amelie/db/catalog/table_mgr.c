
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

void
table_mgr_init(TableMgr*  self, StorageMgr* storage_mgr,
               PartMgrIf* iface,
               void*      iface_arg)
{
	self->storage_mgr = storage_mgr;
	self->iface       = iface;
	self->iface_arg   = iface_arg;
	rel_mgr_init(&self->mgr);
}

void
table_mgr_free(TableMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
table_mgr_create(TableMgr*    self,
                 Tr*          tr,
                 TableConfig* config,
                 bool         if_not_exists)
{
	// make sure table does not exists
	auto current = table_mgr_find(self, &config->user, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("table '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// allocate table
	auto table = table_allocate(config, self->storage_mgr,
	                            self->iface,
	                            self->iface_arg);

	// update tables
	rel_mgr_create(&self->mgr, tr, &table->rel);

	// prepare partitions
	table_open(table);
	return true;
}

void
table_mgr_drop_of(TableMgr* self, Tr* tr, Table* table)
{
	// drop table by object
	rel_mgr_drop(&self->mgr, tr, &table->rel);
}

bool
table_mgr_drop(TableMgr* self, Tr* tr, Str* user, Str* name,
               bool if_exists)
{
	auto table = table_mgr_find(self, user, name, false);
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
table_mgr_truncate(TableMgr* self,
                   Tr*       tr,
                   Str*      user,
                   Str*      name,
                   bool      if_exists)
{
	auto table = table_mgr_find(self, user, name, false);
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

void
table_mgr_dump(TableMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		table_config_write(table->config, buf, 0);
	}
	encode_array_end(buf);
}

Table*
table_mgr_find(TableMgr* self, Str* user, Str* name, bool error_if_not_exists)
{
	auto rel = rel_mgr_find(&self->mgr, user, name, error_if_not_exists);
	return table_of(rel);
}

Table*
table_mgr_find_by(TableMgr* self, Uuid* id, bool error_if_not_exists)
{
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		if (uuid_is(&table->config->id, id))
			return table;
	}
	if (error_if_not_exists)
	{
		char uuid[UUID_SZ];
		uuid_get(id, uuid, sizeof(uuid));
		error("table with uuid '%s' not found", uuid);
	}
	return NULL;
}

void
table_mgr_list(TableMgr* self, Buf* buf, Str* user, Str* name, int flags)
{
	if (user && name)
	{
		// show table
		auto table = table_mgr_find(self, user, name, false);
		if (table)
			table_config_write(table->config, buf, flags);
		else
			encode_null(buf);
		return;
	}

	// show tables
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		if (user && !str_compare_case(&table->config->user, user))
			continue;
		table_config_write(table->config, buf, flags);
	}
	encode_array_end(buf);
}
