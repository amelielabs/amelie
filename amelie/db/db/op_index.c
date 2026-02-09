
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

static void
db_create_index_do(Db* self, Tr* tr, uint8_t* op, int flags)
{
	auto if_not_exists = ddl_if_not_exists(flags);

	Str  name_db;
	Str  name;
	auto pos = table_op_index_create_read(op, &name_db, &name);

	// find table
	auto table = table_mgr_find(&self->catalog.table_mgr, &name_db, &name, false);
	if (! table)
		error("table '%.*s': not exists", str_size(&name),
		      str_of(&name));

	auto config = index_config_read(table_columns(table), &pos);
	errdefer(index_config_free, config);
	keys_set_primary(&config->keys, false);

	// find index
	auto index = table_index_find(table, &config->name, false);
	if (index)
	{
		if (! if_not_exists)
			error("table '%.*s' index '%.*s': already exists",
			      str_size(&table->config->name),
			      str_of(&table->config->name),
			      str_size(&config->name),
			      str_of(&config->name));

		index_config_free(config);
		return;
	}

	// create a list of all pending partitions
	auto list = buf_create();
	defer_buf(list);

	auto table_lock = lock(&table->rel, LOCK_SHARED);

	list_foreach(&engine_main(&table->engine)->list)
	{
		auto part = list_at(Part, link);
		if (! part_index_find(part, &config->name, false))
			buf_write(list, &part->id, sizeof(part->id));
	}

	unlock(table_lock);

	// create partition indexes
	Indexate ix;
	indexate_init(&ix, self);
	auto end = (Id*)list->position;
	auto it  = (Id*)list->start;
	for (; it < end; it++)
	{
		indexate_reset(&ix);
		indexate_run(&ix, &it->id_table, it->id, config);
	}

	// todo: rollback on error

	// todo: exclusive lock

	// attach index to the table
	table_index_add(table, tr, config);
	log_persist_relation(&tr->log, op);
}

void
db_create_index(Db* self, Tr* tr, uint8_t* op, int flags)
{
	auto lock_cp      = lock_system(LOCK_CHECKPOINT, LOCK_EXCLUSIVE);
	auto lock_catalog = lock_system(LOCK_CATALOG, LOCK_SHARED);
	auto on_error = error_catch (
		db_create_index_do(self, tr, op, flags);
	);
	unlock(lock_catalog);
	unlock(lock_cp);

	if (on_error)
		rethrow();
}
