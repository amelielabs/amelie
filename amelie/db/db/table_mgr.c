
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

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
	auto current = table_mgr_find(self, &config->schema, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("table '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// allocate table
	auto table = table_allocate(config, self->part_mgr);

	// save create table operation
	auto op = table_op_create(config);

	// update tables
	relation_mgr_create(&self->mgr, tr, CMD_TABLE_CREATE, &table->rel, op);

	// prepare partitions
	table_open(table);

	// map partitions
	part_list_map(&table->part_list);
	return true;
}

void
table_mgr_drop_of(TableMgr* self, Tr* tr, Table* table)
{
	// save drop table operation
	auto op = table_op_drop(&table->config->schema, &table->config->name);

	// drop table by object
	relation_mgr_drop(&self->mgr, tr, CMD_TABLE_DROP, &table->rel, op);
}

void
table_mgr_drop(TableMgr* self, Tr* tr, Str* schema, Str* name,
               bool if_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}
	table_mgr_drop_of(self, tr, table);
}

static void
truncate_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	// truncate all partitions
	part_list_truncate(&table->part_list);
	buf_free(relation->data);
}

static void
truncate_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	buf_free(relation->data);
}

static LogIf truncate_if =
{
	.commit = truncate_if_commit,
	.abort  = truncate_if_abort
};

void
table_mgr_truncate(TableMgr* self,
                   Tr*       tr,
                   Str*      schema,
                   Str*      name,
                   bool      if_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// save truncate table operation
	auto op = table_op_truncate(schema, name);

	// update table
	log_relation(&tr->log, CMD_TABLE_TRUNCATE, &truncate_if,
	             NULL,
	             &table->rel, op);

	// do nothing (actual truncate will happen on commit)
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
table_mgr_find(TableMgr* self, Str* schema, Str* name,
               bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, schema, name);
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
table_mgr_list(TableMgr* self, Str* schema, Str* name, bool extended)
{
	auto buf = buf_create();
	if (schema && name)
	{
		// show table
		auto table = table_mgr_find(self, schema, name, false);
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
		if (schema && !str_compare(&table->config->schema, schema))
			continue;
		if (extended)
			table_config_write(table->config, buf);
		else
			table_config_write_compact(table->config, buf);
	}
	encode_array_end(buf);
	return buf;
}
