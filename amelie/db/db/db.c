
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
db_init(Db*        self,
        PartAttach attach,
        void*      attach_arg,
        ProcIf*    proc_iface,
        void*      proc_iface_arg)
{
	schema_mgr_init(&self->schema_mgr);
	part_mgr_init(&self->part_mgr, attach, attach_arg);
	table_mgr_init(&self->table_mgr, &self->part_mgr);
	proc_mgr_init(&self->proc_mgr, proc_iface, proc_iface_arg);
	checkpoint_mgr_init(&self->checkpoint_mgr, &db_checkpoint_if, self);
	checkpointer_init(&self->checkpointer, &self->checkpoint_mgr);
	wal_mgr_init(&self->wal_mgr);
}

void
db_free(Db* self)
{
	table_mgr_free(&self->table_mgr);
	part_mgr_free(&self->part_mgr);
	proc_mgr_free(&self->proc_mgr);
	schema_mgr_free(&self->schema_mgr);
	checkpoint_mgr_free(&self->checkpoint_mgr);
	wal_mgr_free(&self->wal_mgr);
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
db_close(Db* self)
{
	// stop checkpointer service
	checkpointer_stop(&self->checkpointer);

	// free tables
	table_mgr_free(&self->table_mgr);

	// free partition mgr
	part_mgr_free(&self->part_mgr);

	// stop wal mgr
	wal_mgr_stop(&self->wal_mgr);
}

Buf*
db_status(Db* self)
{
	// {}
	auto buf = buf_create();
	encode_obj(buf);

	int tables = 0;
	int tables_secondary_indexes = 0;
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		tables++;
		tables_secondary_indexes += (table->config->indexes_count - 1);
	}

	// schemas
	encode_raw(buf, "schemas", 7);
	encode_integer(buf, self->schema_mgr.mgr.list_count);

	// tables
	encode_raw(buf, "tables", 6);
	encode_integer(buf, tables);

	// indexes
	encode_raw(buf, "secondary_indexes", 17);
	encode_integer(buf, tables_secondary_indexes);

	encode_obj_end(buf);
	return buf;
}

Buf*
db_state(Db* self)
{
	unused(self);

	// {}
	auto buf = buf_create();
	encode_obj(buf);

	// version
	encode_raw(buf, "version", 7);
	encode_string(buf, &state()->version.string);

	// directory
	encode_raw(buf, "directory", 9);
	encode_string(buf, &state()->directory.string);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_string(buf, &config()->uuid.string);

	// frontends
	encode_raw(buf, "frontends", 9);
	encode_integer(buf, opt_int_of(&config()->frontends));

	// backends
	encode_raw(buf, "backends", 8);
	encode_integer(buf, opt_int_of(&config()->backends));

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, state_checkpoint());

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, state_lsn());

	// psn
	encode_raw(buf, "psn", 3);
	encode_integer(buf, state_psn());

	// read_only
	encode_raw(buf, "read_only", 9);
	encode_bool(buf, opt_int_of(&state()->read_only));

	encode_obj_end(buf);
	return buf;
}
