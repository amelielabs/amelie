
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

static void
set_aggregated_if_commit(Log* self, LogOp* op)
{
	buf_free(log_handle_of(self, op)->data);
}

static void
set_aggregated_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	auto table = table_of(handle->handle);
	uint8_t* pos = handle->data->start;
	Str  schema;
	Str  name;
	bool value;
	table_op_set_aggregated_read(&pos, &schema, &name, &value);
	table_config_set_aggregated(table->config, !value);
	buf_free(handle->data);
}

static LogIf set_aggregated_if =
{
	.commit = set_aggregated_if_commit,
	.abort  = set_aggregated_if_abort
};

void
table_mgr_set_aggregated(TableMgr* self,
                         Tr*       tr,
                         Str*      schema,
                         Str*      name,
                         bool      if_exists,
                         bool      value)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	if (table->config->aggregated == value)
		return;

	// save set aggregated operation
	auto op = table_op_set_aggregated(schema, name, value);

	// update table
	log_handle(&tr->log, LOG_TABLE_SET_AGGREGATED, &set_aggregated_if,
	           NULL,
	           &table->handle, NULL, op);

	table_config_set_aggregated(table->config, value);
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	buf_free(log_handle_of(self, op)->data);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	auto table = table_of(handle->handle);
	uint8_t* pos = handle->data->start;
	Str schema;
	Str name;
	Str schema_new;
	Str name_new;
	table_op_rename_read(&pos, &schema, &name, &schema_new, &name_new);
	table_config_set_schema(table->config, &schema);
	table_config_set_name(table->config, &name);
	buf_free(handle->data);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

void
table_mgr_rename(TableMgr* self,
                 Tr*       tr,
                 Str*      schema,
                 Str*      name,
                 Str*      schema_new,
                 Str*      name_new,
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

	// ensure new table does not exists
	if (table_mgr_find(self, schema_new, name_new, false))
		error("table '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// save rename table operation
	auto op = table_op_rename(schema, name, schema_new, name_new);

	// update table
	log_handle(&tr->log, LOG_TABLE_RENAME, &rename_if,
	           NULL,
	           &table->handle, NULL, op);

	// set new table name
	if (! str_compare(&table->config->schema, schema_new))
		table_config_set_schema(table->config, schema_new);

	if (! str_compare(&table->config->name, name_new))
		table_config_set_name(table->config, name_new);
}

static void
column_rename_if_commit(Log* self, LogOp* op)
{
	buf_free(log_handle_of(self, op)->data);
}

static void
column_rename_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	Column* column = op->iface_arg;
	uint8_t* pos = handle->data->start;
	Str schema;
	Str name;
	Str name_column;
	Str name_column_new;
	table_op_column_rename_read(&pos, &schema, &name, &name_column,
	                            &name_column_new);
	column_set_name(column, &name_column);
	buf_free(handle->data);
}

static LogIf column_rename_if =
{
	.commit = column_rename_if_commit,
	.abort  = column_rename_if_abort
};

void
table_mgr_column_rename(TableMgr* self,
                        Tr*       tr,
                        Str*      schema,
                        Str*      name,
                        Str*      name_column,
                        Str*      name_column_new,
                        bool      if_exists)
{
	// find table
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// find column
	auto column = columns_find(&table->config->columns, name_column);
	if (! column)
	{
		error("table '%.*s': column '%.*s' not exists", str_size(name),
		      str_of(name),
		      str_size(name_column),
		      str_of(name_column));
	}

	// ensure new column does not exists
	auto ref = columns_find(&table->config->columns, name_column_new);
	if (ref)
	{
		error("table '%.*s': column '%.*s' already exists", str_size(name),
		      str_of(name),
		      str_size(name_column_new),
		      str_of(name_column_new));
	}

	// save rename table operation
	auto op = table_op_column_rename(schema, name, name_column, name_column_new);

	// update table
	log_handle(&tr->log, LOG_TABLE_COLUMN_RENAME, &column_rename_if,
	           column,
	           &table->handle, NULL, op);

	// set column name
	column_set_name(column, name_column_new);
}

static void
column_add_if_commit(Log* self, LogOp* op)
{
	// swap former table with a new one
	TableMgr* mgr = op->iface_arg;
	auto handle = log_handle_of(self, op);
	auto table  = table_of(handle->handle);
	auto prev   = table_mgr_find(mgr, &table->config->schema,
	                             &table->config->name,
	                              true);
	handle_mgr_replace(&mgr->mgr, &prev->handle, &table->handle);

	// free previous table and unmap partitions
	table_free(prev);

	// remap new table partitions (partitions has same ids)
	part_list_map(&table->part_list);
	buf_free(handle->data);
}

static void
column_add_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	auto table = table_of(handle->handle);
	table_free(table);
	buf_free(handle->data);
}

static LogIf column_add_if =
{
	.commit = column_add_if_commit,
	.abort  = column_add_if_abort
};

Table*
table_mgr_column_add(TableMgr* self,
                     Tr*       tr,
                     Str*      schema,
                     Str*      name,
                     Column*   column,
                     bool      if_not_exists)
{
	auto table = table_mgr_find(self, schema, name, true);

	// ensure column does not exists
	if (columns_find(&table->config->columns, &column->name))
	{
		if (! if_not_exists)
			error("table '%.*s': column '%.*s' already exists", str_size(name),
			      str_of(name),
			      str_size(&column->name),
			      str_of(&column->name));
		return NULL;
	}

	// physical column require a new table

	// allocate new table
	auto table_new = table_allocate(table->config, &self->part_mgr);

	// add new column
	auto column_new = column_copy(column);
	columns_add(&table_new->config->columns, column_new);

	// save operation
	auto op = table_op_column_add(&table->config->schema, &table->config->name, column);

	// update log (old table is still present)
	log_handle(&tr->log, LOG_TABLE_COLUMN_ADD, &column_add_if, self,
	           &table_new->handle, NULL, op);

	table_open(table_new);
	return table_new;
}

Table*
table_mgr_column_drop(TableMgr* self,
                      Tr*       tr,
                      Str*      schema,
                      Str*      name,
                      Str*      name_column,
                      bool      if_exists)
{
	auto table = table_mgr_find(self, schema, name, true);

	auto column = columns_find(&table->config->columns, name_column);
	if (! column)
	{
		if (! if_exists)
			error("table '%.*s': column '%.*s' not exists", str_size(name),
			      str_of(name),
			      str_size(name_column),
			      str_of(name_column));
		return NULL;
	}

	// ensure column currently not used as a key
	if (column->key)
		error("table '%.*s': column '%.*s' is a key", str_size(name),
		      str_of(name),
		      str_size(name_column),
		      str_of(name_column));

	// physical column require new table rebuild

	// allocate new table
	auto table_new = table_allocate(table->config, &self->part_mgr);

	// delete and reorder columns and update keys
	auto column_new = columns_find(&table_new->config->columns, &column->name);
	columns_del(&table_new->config->columns, column_new);
	column_free(column_new);

	list_foreach(&table_new->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		keys_update(&config->keys);
	}

	// save operation
	auto op = table_op_column_drop(&table->config->schema, &table->config->name,
	                               &column->name);

	// update log (old table is still present)
	log_handle(&tr->log, LOG_TABLE_COLUMN_DROP, &column_add_if, self,
	           &table_new->handle, NULL, op);

	table_open(table_new);
	return table_new;
}
