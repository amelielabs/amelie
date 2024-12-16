
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
table_mgr_init(TableMgr* self, PartMapper mapper, void* mapper_arg)
{
	handle_mgr_init(&self->mgr);
	part_mgr_init(&self->part_mgr, mapper, mapper_arg);
}

void
table_mgr_free(TableMgr* self)
{
	handle_mgr_free(&self->mgr);
	part_mgr_free(&self->part_mgr);
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
	auto table = table_allocate(config, &self->part_mgr);

	// save create table operation
	auto op = table_op_create(config);

	// update tables
	handle_mgr_create(&self->mgr, tr, LOG_TABLE_CREATE, &table->handle, op);

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
	handle_mgr_drop(&self->mgr, tr, LOG_TABLE_DROP, &table->handle, op);
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
	auto handle = log_handle_of(self, op);
	auto table = table_of(handle->handle);
	// truncate all partitions
	part_list_truncate(&table->part_list);
	buf_free(handle->data);
}

static void
truncate_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	buf_free(handle->data);
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
	log_handle(&tr->log, LOG_TABLE_TRUNCATE, &truncate_if,
	           NULL,
	           &table->handle, NULL, op);

	// do nothing (actual truncate will happen on commit)
}

void
table_mgr_dump(TableMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		table_config_write(table->config, buf);
	}
	encode_array_end(buf);
}

Table*
table_mgr_find(TableMgr* self, Str* schema, Str* name,
               bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, schema, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return table_of(handle);
}

Buf*
table_mgr_list(TableMgr* self, Str* name)
{
	auto buf = buf_create();
	if (name)
	{
		// todo: parse name as schema.target
		Str schema;
		str_set(&schema, "public", 6);
		auto table = table_mgr_find(self, &schema, name, false);
		if (! table)
			encode_null(buf);
		else
			table_config_write(table->config, buf);
	} else
	{
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto table = table_of(list_at(Handle, link));
			table_config_write(table->config, buf);
		}
		encode_array_end(buf);
	}
	return buf;
}

Part*
table_mgr_find_partition(TableMgr* self, uint64_t id)
{
	return part_mgr_find(&self->part_mgr, id);
}
