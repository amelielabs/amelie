
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
#include <monotone_wal.h>
#include <monotone_db.h>

void
table_mgr_init(TableMgr* self, CompactMgr* compact_mgr)
{
	self->compact_mgr = compact_mgr;
	handle_mgr_init(&self->tables);
	handle_mgr_set_free(&self->tables, table_destructor, NULL);
}

void
table_mgr_free(TableMgr* self)
{
	handle_mgr_free(&self->tables);
}

void
table_mgr_gc(TableMgr* self, uint64_t snapshot_min)
{
	handle_mgr_gc(&self->tables, snapshot_min);
}

void
table_mgr_create(TableMgr*    self,
                 Transaction* trx,
                 TableConfig* config,
                 bool         if_not_exists)
{
	// make sure table does not exists
	auto current = handle_mgr_get(&self->tables, &config->name);
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

	// create new storage
	table->storage = storage_create(table->config->config, self->compact_mgr);
	storage_open(table->storage, true);

	// save create table operation
	auto op = table_op_create_table(config);

	// update tables
	mvcc_write_handle(trx, LOG_CREATE_TABLE, &self->tables, &table->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

static void
table_mgr_drop_table(TableMgr* self, Transaction* trx, Table* table)
{
	// create new drop table
	auto update = table_allocate(table->config);
	update->handle.drop = true;
	guard(guard, table_free, update);

	// save drop table operation
	auto op = table_op_drop_table(&table->config->name);

	// update tables
	mvcc_write_handle(trx, LOG_DROP_TABLE, &self->tables, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

void
table_mgr_drop(TableMgr* self, Transaction* trx, Str* name, bool if_exists)
{
	auto current = handle_mgr_get(&self->tables, name);
	if (! current)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}
	auto table = table_of(current);
	table_mgr_drop_table(self, trx, table);
}

void
table_mgr_alter(TableMgr*    self,
                Transaction* trx ,
                Str*         name,
                TableConfig* config,
                bool         if_exists)
{
	auto current = handle_mgr_get(&self->tables, name);
	if (! current)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}
	auto table = table_of(current);

	// allocate new table
	auto update = table_allocate(config);
	guard(guard, table_free, update);

	// if name changed, drop previous table
	if (! str_compare(&config->name, name))
	{
		// ensure new table does not exists
		if (table_mgr_find(self, &config->name, false))
			error("table '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));

		table_mgr_drop_table(self, trx, table);
	}

	// use previous table storage
	update->storage = table->storage;
	storage_ref(update->storage);

	// save alter table operation
	auto op = table_op_alter_table(name, config);

	// update tables
	mvcc_write_handle(trx, LOG_ALTER_TABLE, &self->tables, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

Table*
table_mgr_find(TableMgr* self, Str* name, bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->tables, name);
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
	list_foreach(&self->tables.list)
	{
		auto head = list_at(Handle, link);
		while (head)
		{
			auto table = table_of(head);
			if (uuid_compare(&table->config->config->id, id))
				return table;
			head = head->prev;
		}
	}
	return NULL;
}

Buf*
table_mgr_show(TableMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_map32(buf, 0);

	int count = 0;
	list_foreach(&self->tables.list)
	{
		auto head = list_at(Handle, link);
		if (head->drop)
			continue;

		// "name": { config }
		auto table = table_of(head);
		encode_string(buf, &table->config->name);
		table_config_write(table->config, buf);

		count++;
	}

	// update count
	uint8_t* map_pos = buf->start + sizeof(Msg);
	data_write_map32(&map_pos, count);

	msg_end(buf);
	return buf;
}
