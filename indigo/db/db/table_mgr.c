
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>

void
table_mgr_init(TableMgr* self)
{
	handle_mgr_init(&self->mgr);
}

void
table_mgr_free(TableMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
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
		return;
	}

	// allocate table
	auto table = table_allocate(config);
	guard(guard, table_free, table);

	// save create table operation
	auto op = table_op_create(config);

	// update tables
	handle_mgr_create(&self->mgr, trx, LOG_TABLE_CREATE, &table->handle, op);

	buf_unpin(op);
	unguard(&guard);

	// prepare storage manager
	table_open(table);
}

void
table_mgr_drop_of(TableMgr* self, Transaction* trx, Table* table)
{
	// save drop table operation
	auto op = table_op_drop(&table->config->schema, &table->config->name);

	// drop table by object
	handle_mgr_drop(&self->mgr, trx, LOG_TABLE_DROP, &table->handle, op);
	buf_unpin(op);
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
table_mgr_rename_abort(LogOp* op)
{
	auto self = table_of(op->handle.handle);
	uint8_t* pos = op->handle.data->start;
	Str schema;
	Str name;
	Str schema_new;
	Str name_new;
	table_op_rename_read(&pos, &schema, &name, &schema_new, &name_new);
	table_config_set_schema(self->config, &schema);
	table_config_set_name(self->config, &name);
	buf_free(op->handle.data);
}

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

	// update table
	handle_mgr_alter(&self->mgr, trx, LOG_TABLE_RENAME, &table->handle, op,
	                 table_mgr_rename_abort, NULL);
	buf_unpin(op);

	// set new table name
	if (! str_compare(&table->config->schema, schema_new))
		table_config_set_schema(table->config, schema_new);

	if (! str_compare(&table->config->name, name_new))
		table_config_set_name(table->config, name_new);
}

void
table_mgr_dump(TableMgr* self, Buf* buf)
{
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		table_config_write(table->config, buf);
	}
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

Table*
table_mgr_find_by_id(TableMgr* self, Uuid* id)
{
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (uuid_compare(&table->config->id, id))
			return table;
	}
	return NULL;
}

Buf*
table_mgr_list(TableMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		// {}
		auto table = table_of(list_at(Handle, link));
		table_config_write(table->config, buf);
	}
	msg_end(buf);
	return buf;
}

Buf*
table_mgr_list_partitions(TableMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		storage_mgr_list(&table->storage_mgr, buf);
	}
	msg_end(buf);
	return buf;
}
