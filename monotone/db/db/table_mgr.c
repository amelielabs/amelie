
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
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>
#include <monotone_storage.h>
#include <monotone_db.h>

void
table_mgr_init(TableMgr* self)
{
	table_cache_init(&self->cache);
}

void
table_mgr_free(TableMgr* self)
{
	table_cache_free(&self->cache);
}

void
table_mgr_create(TableMgr*    self,
                 Transaction* trx,
                 TableConfig* config,
                 bool         if_not_exists)
{
	// make sure table does not exists
	auto current = table_cache_find(&self->cache, &config->name, false);
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
	auto op = table_op_create_table(config);

	// update tables
	mvcc_write_handle(trx, LOG_CREATE_TABLE, &self->cache.cache, &table->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

static void
table_mgr_drop_table(TableMgr* self, Transaction* trx, Table* table)
{
	// save drop table operation
	auto op = table_op_drop_table(&table->config->name);

	// drop table by name
	Handle drop;
	handle_init(&drop);
	drop.name = &table->config->name;
	mvcc_write_handle(trx, LOG_DROP_TABLE, &self->cache.cache, &drop, op);

	buf_unpin(op);
}

void
table_mgr_drop(TableMgr* self, Transaction* trx, Str* name, bool if_exists)
{
	auto table = table_cache_find(&self->cache, name, false);
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
                Str*         name,
                TableConfig* config,
                bool         if_exists)
{
	auto table = table_cache_find(&self->cache, name, false);
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

	// if name changed, drop previous table
	if (! str_compare(&config->name, name))
	{
		// ensure new table does not exists
		if (table_cache_find(&self->cache, &config->name, false))
			error("table '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));

		table_mgr_drop_table(self, trx, table);
	}

	// save alter table operation
	auto op = table_op_alter_table(name, config);

	// update tables
	mvcc_write_handle(trx, LOG_ALTER_TABLE, &self->cache.cache, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
}
