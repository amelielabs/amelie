
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>

void
table_mgr_init(TableMgr* self, PartMgr* part_mgr)
{
	self->part_mgr = part_mgr;
	relation_mgr_init(&self->mgr);
}

void
table_mgr_free(TableMgr* self)
{
	relation_mgr_free(&self->mgr);
}

bool
table_mgr_create(TableMgr*    self,
                 Tr*          tr,
                 TableConfig* config,
                 bool         if_not_exists)
{
	// make sure table does not exists
	auto current = table_mgr_find(self, &config->db, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("table '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// allocate table
	auto table = table_allocate(config, self->part_mgr);

	// update tables
	relation_mgr_create(&self->mgr, tr, &table->rel);

	// prepare partitions
	table_open(table);

	// map partitions
	part_list_map(&table->part_list);
	return true;
}

void
table_mgr_drop_of(TableMgr* self, Tr* tr, Table* table)
{
	// drop table by object
	relation_mgr_drop(&self->mgr, tr, &table->rel);
}

bool
table_mgr_drop(TableMgr* self, Tr* tr, Str* db, Str* name,
               bool if_exists)
{
	auto table = table_mgr_find(self, db, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	table_mgr_drop_of(self, tr, table);
	return true;
}

static void
truncate_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	// truncate all partitions
	part_list_truncate(&table->part_list);
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
                   Str*      db,
                   Str*      name,
                   bool      if_exists)
{
	auto table = table_mgr_find(self, db, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// update table
	log_relation(&tr->log, &truncate_if, NULL, &table->rel);

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
		auto table = table_of(list_at(Relation, link));
		table_config_write(table->config, buf);
	}
	encode_array_end(buf);
}

Table*
table_mgr_find(TableMgr* self, Str* db, Str* name,
               bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, db, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return table_of(relation);
}

Buf*
table_mgr_list(TableMgr* self, Str* db, Str* name, bool extended)
{
	auto buf = buf_create();
	if (db && name)
	{
		// show table
		auto table = table_mgr_find(self, db, name, false);
		if (table) {
			if (extended)
				table_config_write(table->config, buf);
			else
				table_config_write_compact(table->config, buf);
		} else {
			encode_null(buf);
		}
		return buf;
	}

	// show tables
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		if (db && !str_compare_case(&table->config->db, db))
			continue;
		if (extended)
			table_config_write(table->config, buf);
		else
			table_config_write_compact(table->config, buf);
	}
	encode_array_end(buf);
	return buf;
}
