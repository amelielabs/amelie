
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

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
	handle_mgr_write(&self->mgr, trx, LOG_TABLE_CREATE, &table->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

static void
table_mgr_drop_table(TableMgr* self, Transaction* trx, Table* table)
{
	// save drop table operation
	auto op = table_op_drop(&table->config->schema, &table->config->name);

	// drop table by name
	Handle drop;
	handle_init(&drop);
	handle_set_schema(&drop, &table->config->schema);
	handle_set_name(&drop, &table->config->name);

	handle_mgr_write(&self->mgr, trx, LOG_TABLE_DROP, &drop, op);

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
	table_mgr_drop_table(self, trx, table);
}

void
table_mgr_alter(TableMgr*    self,
                Transaction* trx,
                Str*         schema,
                Str*         name,
                TableConfig* config,
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

	// allocate new table
	auto update = table_allocate(config);
	guard(guard, table_free, update);

	// if schema or name changed, drop previous table
	if (!str_compare(&config->schema, schema) ||
	    !str_compare(&config->name, name))
	{
		// ensure new table does not exists
		if (table_mgr_find(self, &config->schema, &config->name, false))
			error("table '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));

		table_mgr_drop_table(self, trx, table);
	}

	// save alter table operation
	auto op = table_op_alter(schema, name, config);

	// update tables
	handle_mgr_write(&self->mgr, trx, LOG_TABLE_ALTER, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
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
