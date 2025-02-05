
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
db_init(Db*        self,
        PartMapper mapper,
        void*      mapper_arg,
        NodeIf*    node_iface,
        void*      node_iface_arg)
{
	schema_mgr_init(&self->schema_mgr);
	table_mgr_init(&self->table_mgr, mapper, mapper_arg);
	node_mgr_init(&self->node_mgr, node_iface, node_iface_arg);
	checkpoint_mgr_init(&self->checkpoint_mgr, &db_checkpoint_if, self);
	checkpointer_init(&self->checkpointer, &self->checkpoint_mgr);
	wal_init(&self->wal);
}

void
db_free(Db* self)
{
	table_mgr_free(&self->table_mgr);
	node_mgr_free(&self->node_mgr);
	schema_mgr_free(&self->schema_mgr);
	checkpoint_mgr_free(&self->checkpoint_mgr);
	wal_free(&self->wal);
}

static void
db_create_system_schema(Db* self, const char* schema, bool create)
{
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		// create system schema
		Str name;
		str_init(&name);
		str_set_cstr(&name, schema);
		auto config = schema_config_allocate();
		defer(schema_config_free, config);
		schema_config_set_name(config, &name);
		schema_config_set_system(config, true);
		schema_config_set_create(config, create);
		schema_mgr_create(&self->schema_mgr, &tr, config, false);

		// commit
		tr_commit(&tr);
	);
	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}

void
db_open(Db* self)
{
	// prepare system schemas
	db_create_system_schema(self, "system", false);
	db_create_system_schema(self, "public", true);

	// read directory and restore last checkpoint catalog
	// (schemas, tables)
	checkpoint_mgr_open(&self->checkpoint_mgr);
}

void
db_close(Db* self, bool fast)
{
	// stop checkpointer service
	checkpointer_stop(&self->checkpointer);

	// free tables
	if (! fast)
		table_mgr_free(&self->table_mgr);
	else
		handle_mgr_init(&self->table_mgr.mgr);

	// free nodes
	node_mgr_free(&self->node_mgr);
}

Buf*
db_status(Db* self)
{
	// {}
	auto buf = buf_create();
	encode_obj(buf);

	int tables = 0;
	int tables_shared = 0;
	int tables_secondary_indexes = 0;
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (table->config->shared)
			tables_shared++;
		else
			tables++;
		tables_secondary_indexes += (table->config->indexes_count - 1);
	}

	// schemas
	encode_raw(buf, "schemas", 7);
	encode_integer(buf, self->schema_mgr.mgr.list_count);

	// tables
	encode_raw(buf, "tables", 6);
	encode_integer(buf, tables);

	// shared_tables
	encode_raw(buf, "tables_shared", 13);
	encode_integer(buf, tables_shared);

	// indexes
	encode_raw(buf, "secondary_indexes", 17);
	encode_integer(buf, tables_secondary_indexes);

	encode_obj_end(buf);
	return buf;
}
