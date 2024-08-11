
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
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
                 Transaction* trx,
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
	guard(table_free, table);

	// save create table operation
	auto op = table_op_create(config);
	guard_buf(op);

	// update tables
	handle_mgr_create(&self->mgr, trx, LOG_TABLE_CREATE, &table->handle, op);

	unguard();
	unguard();

	// prepare partitions
	table_open(table);

	// map partition
	part_list_map(&table->part_list);
	return true;
}

void
table_mgr_drop_of(TableMgr* self, Transaction* trx, Table* table)
{
	// save drop table operation
	auto op = table_op_drop(&table->config->schema, &table->config->name);
	guard_buf(op);

	// drop table by object
	handle_mgr_drop(&self->mgr, trx, LOG_TABLE_DROP, &table->handle, op);
	unguard();
}

void
table_mgr_drop(TableMgr* self, Transaction* trx, Str* schema, Str* name,
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
	table_mgr_drop_of(self, trx, table);
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
table_mgr_rename(TableMgr*    self,
                 Transaction* trx,
                 Str*         schema,
                 Str*         name,
                 Str*         schema_new,
                 Str*         name_new,
                 bool         if_exists)
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
	guard_buf(op);

	// update table
	log_handle(&trx->log, LOG_TABLE_RENAME, &rename_if,
	           NULL,
	           &table->handle, op);
	unguard();

	// set new table name
	if (! str_compare(&table->config->schema, schema_new))
		table_config_set_schema(table->config, schema_new);

	if (! str_compare(&table->config->name, name_new))
		table_config_set_name(table->config, name_new);
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
table_mgr_truncate(TableMgr*    self,
                   Transaction* trx,
                   Str*         schema,
                   Str*         name,
                   bool         if_exists)
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
	guard_buf(op);

	// update table
	log_handle(&trx->log, LOG_TABLE_TRUNCATE, &truncate_if,
	           NULL,
	           &table->handle, op);
	unguard();

	// do nothing (actual truncate will happen on commit)
}

static void
rename_column_if_commit(Log* self, LogOp* op)
{
	buf_free(log_handle_of(self, op)->data);
}

static void
rename_column_if_abort(Log* self, LogOp* op)
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

static LogIf rename_column_if =
{
	.commit = rename_column_if_commit,
	.abort  = rename_column_if_abort
};

void
table_mgr_column_rename(TableMgr*    self,
                        Transaction* trx,
                        Str*         schema,
                        Str*         name,
                        Str*         name_column,
                        Str*         name_column_new,
                        bool         if_exists)
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
	guard_buf(op);

	// update table
	log_handle(&trx->log, LOG_TABLE_COLUMN_RENAME, &rename_column_if,
	           column,
	           &table->handle, op);
	unguard();

	// set column name
	column_set_name(column, name_column_new);
}

static void
column_if_commit(Log* self, LogOp* op)
{
	TableMgr* mgr = op->iface_arg;
	// swap former table with a new one
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
column_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	auto table = table_of(handle->handle);
	table_free(table);
	buf_free(handle->data);
}

static LogIf column_if =
{
	.commit = column_if_commit,
	.abort  = column_if_abort
};

Table*
table_mgr_column_add(TableMgr*    self,
                     Transaction* trx,
                     Str*         schema,
                     Str*         name,
                     Column*      column,
                     bool         if_not_exists)
{
	auto table = table_mgr_find(self, schema, name, true);

	auto ref = columns_find(&table->config->columns, &column->name);
	if (ref)
	{
		if (! if_not_exists)
			error("table '%.*s': column '%.*s' already exists", str_size(name),
			      str_of(name),
			      str_size(&ref->name),
			      str_of(&ref->name));
		return NULL;
	}

	// allocate new table
	auto table_new = table_allocate(table->config, &self->part_mgr);
	guard(table_free, table_new);

	// add new column
	auto column_new = column_copy(column);
	guard(column_free, column_new);
	columns_add(&table_new->config->columns, column_new);
	unguard();

	// save operation
	auto op = table_op_column_add(schema, name, column);
	guard_buf(op);

	// update log (old table is still present)
	log_handle(&trx->log, LOG_TABLE_COLUMN_ADD, &column_if, self,
	           &table_new->handle, op);
	unguard();
	unguard();

	table_open(table_new);
	return table_new;
}

Table*
table_mgr_column_drop(TableMgr*    self,
                      Transaction* trx,
                      Str*         schema,
                      Str*         name,
                      Str*         name_column,
                      bool         if_exists)
{
	auto table = table_mgr_find(self, schema, name, true);

	auto ref = columns_find(&table->config->columns, name_column);
	if (! ref)
	{
		if (! if_exists)
			error("table '%.*s': column '%.*s' not exists", str_size(name),
			      str_of(name),
			      str_size(name_column),
			      str_of(name_column));
		return NULL;
	}

	// ensure column currently not used as a key
	if (ref->key)
		error("table '%.*s': column '%.*s' is a key", str_size(name),
		      str_of(name),
		      str_size(name_column),
		      str_of(name_column));

	// allocate new table
	auto table_new = table_allocate(table->config, &self->part_mgr);
	guard(table_free, table_new);

	// delete and reorder columns and update keys
	columns_del(&table_new->config->columns, ref->order);
	list_foreach(&table_new->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		keys_update(&config->keys);
	}

	// save operation
	auto op = table_op_column_drop(schema, name, name_column);
	guard_buf(op);

	// update log (old table is still present)
	log_handle(&trx->log, LOG_TABLE_COLUMN_DROP, &column_if, self,
	           &table_new->handle, op);
	unguard();
	unguard();

	table_open(table_new);
	return table_new;
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
table_mgr_list(TableMgr* self)
{
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		table_config_write(table->config, buf);
	}
	encode_array_end(buf);
	return buf_end(buf);
}

Part*
table_mgr_find_partition(TableMgr* self, uint64_t id)
{
	return part_mgr_find(&self->part_mgr, id);
}
