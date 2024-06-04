
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>

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
	auto table = table_allocate(config);
	guard(table_free, table);

	// save create table operation
	auto op = table_op_create(config);
	guard_buf(op);

	// update tables
	handle_mgr_create(&self->mgr, trx, LOG_TABLE_CREATE, &table->handle, op);

	unguard();
	unguard();

	// prepare partition manager
	table_open(table);
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
	guard_buf(op);

	// update table
	handle_mgr_alter(&self->mgr, trx, LOG_TABLE_RENAME, &table->handle, op,
	                 table_mgr_rename_abort, NULL);
	unguard();

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
	list_foreach(&self->mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		auto part  = part_list_find(&table->part_list, id);
		if (part)
			return part;
	}
	return NULL;
}
