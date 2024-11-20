
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
#include <amelie_value.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

static Buf*
db_checkpoint_catalog_dump(void* arg)
{
	// { nodes, schemas, tables, views }
	Db* self = arg;
	auto buf = buf_create();
	encode_obj(buf);

	encode_raw(buf, "nodes", 5);
	node_mgr_dump(&self->node_mgr, buf);

	encode_raw(buf, "schemas", 7);
	schema_mgr_dump(&self->schema_mgr, buf);

	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	encode_raw(buf, "views", 5);
	view_mgr_dump(&self->view_mgr, buf);

	encode_obj_end(buf);
	return buf;
}

enum
{
	RESTORE_NODE,
	RESTORE_SCHEMA,
	RESTORE_TABLE,
	RESTORE_VIEW
};

static void
restore_object(Db* self, int type, uint8_t** pos)
{
	Tr tr;
	tr_init(&tr);
	guard(tr_free, &tr);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		tr_begin(&tr);

		switch (type) {
		case RESTORE_NODE:
		{
			// read node config
			auto config = node_config_read(pos);
			guard(node_config_free, config);

			// create node
			node_mgr_create(&self->node_mgr, &tr, config, false);
			break;
		}
		case RESTORE_SCHEMA:
		{
			// read schema config
			auto config = schema_config_read(pos);
			guard(schema_config_free, config);

			// create schema
			schema_mgr_create(&self->schema_mgr, &tr, config, false);
			break;
		}
		case RESTORE_TABLE:
		{
			// read table config
			auto config = table_config_read(pos);
			guard(table_config_free, config);

			// create table
			table_mgr_create(&self->table_mgr, &tr, config, false);
			break;
		}
		case RESTORE_VIEW:
		{
			// read view config
			auto config = view_config_read(pos);
			guard(view_config_free, config);

			// create view
			view_mgr_create(&self->view_mgr, &tr, config, false);
			break;
		}
		}
	}

	if (leave(&e))
	{
		tr_abort(&tr);
		rethrow();
	}

	// commit
	tr_commit(&tr);
}

static void
db_checkpoint_catalog_restore(uint8_t** pos, void* arg)
{
	// { nodes, schemas, tables, views }
	Db* self = arg;

	uint8_t* pos_nodes   = NULL;
	uint8_t* pos_schemas = NULL;
	uint8_t* pos_tables  = NULL;
	uint8_t* pos_views   = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "nodes",   &pos_nodes   },
		{ DECODE_ARRAY, "schemas", &pos_schemas },
		{ DECODE_ARRAY, "tables",  &pos_tables  },
		{ DECODE_ARRAY, "views",   &pos_views   },
		{ 0,             NULL,      NULL        },
	};
	decode_obj(obj, "catalog", pos);

	// nodes
	json_read_array(&pos_nodes);
	while (! json_read_array_end(&pos_nodes))
		restore_object(self, RESTORE_NODE, &pos_nodes);

	// schemas
	json_read_array(&pos_schemas);
	while (! json_read_array_end(&pos_schemas))
		restore_object(self, RESTORE_SCHEMA, &pos_schemas);

	// tables
	json_read_array(&pos_tables);
	while (! json_read_array_end(&pos_tables))
		restore_object(self, RESTORE_TABLE, &pos_tables);

	// views
	json_read_array(&pos_views);
	while (! json_read_array_end(&pos_views))
		restore_object(self, RESTORE_VIEW, &pos_views);
}

static void
db_checkpoint_add(Checkpoint* cp, void* arg)
{
	Db* self = arg;
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
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
	wal_gc(&self->wal, config_checkpoint());
}

CheckpointIf db_checkpoint_if =
{
	.catalog_dump    = db_checkpoint_catalog_dump,
	.catalog_restore = db_checkpoint_catalog_restore,
	.add             = db_checkpoint_add,
	.complete        = db_checkpoint_complete
};
