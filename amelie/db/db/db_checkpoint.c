
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

static Buf*
db_checkpoint_catalog_dump(void* arg)
{
	// { schemas, tables, procs }
	Db* self = arg;
	auto buf = buf_create();
	encode_obj(buf);

	encode_raw(buf, "schemas", 7);
	schema_mgr_dump(&self->schema_mgr, buf);

	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	encode_raw(buf, "procs", 5);
	proc_mgr_dump(&self->proc_mgr, buf);

	encode_obj_end(buf);
	return buf;
}

enum
{
	RESTORE_SCHEMA,
	RESTORE_TABLE,
	RESTORE_PROC
};

static void
restore_replay(Db* self, Tr* tr, int type, uint8_t** pos)
{
	switch (type) {
	case RESTORE_SCHEMA:
	{
		// read schema config
		auto config = schema_config_read(pos);
		defer(schema_config_free, config);

		// create schema
		schema_mgr_create(&self->schema_mgr, tr, config, false);
		break;
	}
	case RESTORE_TABLE:
	{
		// read table config
		auto config = table_config_read(pos);
		defer(table_config_free, config);

		// create table
		table_mgr_create(&self->table_mgr, tr, config, false);
		break;
	}
	case RESTORE_PROC:
	{
		// read procedure config
		auto config = proc_config_read(pos);
		defer(proc_config_free, config);

		// create udf
		proc_mgr_create(&self->proc_mgr, tr, config, false);
		break;
	}
	}
}

static void
restore_object(Db* self, int type, uint8_t** pos)
{
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	if (error_catch
	(
		// start transaction
		tr_begin(&tr);

		restore_replay(self, &tr, type, pos);

		// commit
		tr_commit(&tr);
	)) {
		tr_abort(&tr);
		rethrow();
	}
}

static void
db_checkpoint_catalog_restore(uint8_t** pos, void* arg)
{
	// { schemas, tables, procs }
	Db* self = arg;
	uint8_t* pos_schemas = NULL;
	uint8_t* pos_tables  = NULL;
	uint8_t* pos_procs   = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "schemas", &pos_schemas },
		{ DECODE_ARRAY, "tables",  &pos_tables  },
		{ DECODE_ARRAY, "procs",   &pos_procs   },
		{ 0,             NULL,      NULL        },
	};
	decode_obj(obj, "catalog", pos);

	// schemas
	json_read_array(&pos_schemas);
	while (! json_read_array_end(&pos_schemas))
		restore_object(self, RESTORE_SCHEMA, &pos_schemas);

	// tables
	json_read_array(&pos_tables);
	while (! json_read_array_end(&pos_tables))
		restore_object(self, RESTORE_TABLE, &pos_tables);

	// procs
	json_read_array(&pos_procs);
	while (! json_read_array_end(&pos_procs))
		restore_object(self, RESTORE_PROC, &pos_procs);
}

static void
db_checkpoint_add(Checkpoint* cp, void* arg)
{
	Db* self = arg;
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			checkpoint_add(cp, part);
		}
	}
}

static void
db_checkpoint_complete(void* arg)
{
	Db* self = arg;
	// gc
	checkpoint_mgr_gc(&self->checkpoint_mgr);
	wal_mgr_gc(&self->wal_mgr);
}

CheckpointIf db_checkpoint_if =
{
	.catalog_dump    = db_checkpoint_catalog_dump,
	.catalog_restore = db_checkpoint_catalog_restore,
	.add             = db_checkpoint_add,
	.complete        = db_checkpoint_complete
};
