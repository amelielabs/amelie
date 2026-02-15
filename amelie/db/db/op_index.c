
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

void
db_create_index(Db* self, Tr* tr, uint8_t* op, int flags)
{
	auto if_not_exists = ddl_if_not_exists(flags);

	Str  name_db;
	Str  name;
	auto pos = table_op_index_create_read(op, &name_db, &name);

	// take shared catalog lock
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);
	defer(unlock, lock_catalog);

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

	// incrementally create indexes on every table partition
	Indexate ix;
	indexate_init(&ix, self);
	while (indexate_next(&ix, table, config));

	unlock(lock_catalog);

	// take exclusive catalog lock (HELD till completion)
	lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);

	// attach index to the table
	table_index_add(table, tr, config);
	log_persist_relation(&tr->log, op);
}
