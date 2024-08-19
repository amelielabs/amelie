
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
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
	// { schemas, tables, views }
	Db* self = arg;
	auto buf = buf_begin();
	encode_map(buf);

	encode_raw(buf, "schemas", 7);
	schema_mgr_dump(&self->schema_mgr, buf);

	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	encode_raw(buf, "views", 5);
	view_mgr_dump(&self->view_mgr, buf);

	encode_map_end(buf);
	return buf_end(buf);
}

enum
{
	RESTORE_SCHEMA,
	RESTORE_TABLE,
	RESTORE_VIEW
};

static void
restore_object(Db* self, int type, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);

		switch (type) {
		case RESTORE_SCHEMA:
		{
			// read schema config
			auto config = schema_config_read(pos);
			guard(schema_config_free, config);

			// create schema
			schema_mgr_create(&self->schema_mgr, &trx, config, false);
			break;
		}
		case RESTORE_TABLE:
		{
			// read table config
			auto config = table_config_read(pos);
			guard(table_config_free, config);

			// create table
			table_mgr_create(&self->table_mgr, &trx, config, false);
			break;
		}
		case RESTORE_VIEW:
		{
			// read view config
			auto config = view_config_read(pos);
			guard(view_config_free, config);

			// create view
			view_mgr_create(&self->view_mgr, &trx, config, false);
			break;
		}
		}
	}

	if (leave(&e))
	{
		transaction_abort(&trx);
		rethrow();
	}

	// commit
	transaction_commit(&trx);
}

static void
db_checkpoint_catalog_restore(uint8_t** pos, void* arg)
{
	// { schemas, tables, views }
	Db* self = arg;

	uint8_t* pos_schemas  = NULL;
	uint8_t* pos_tables = NULL;
	uint8_t* pos_views = NULL;
	Decode map[] =
	{
		{ DECODE_ARRAY, "schemas", &pos_schemas },
		{ DECODE_ARRAY, "tables",  &pos_tables  },
		{ DECODE_ARRAY, "views",   &pos_views   },
		{ 0,             NULL,      NULL        },
	};
	decode_map(map, "catalog", pos);

	// schemas
	data_read_array(&pos_schemas);
	while (! data_read_array_end(&pos_schemas))
		restore_object(self, RESTORE_SCHEMA, &pos_schemas);

	// tables
	data_read_array(&pos_tables);
	while (! data_read_array_end(&pos_tables))
		restore_object(self, RESTORE_TABLE, &pos_tables);

	// views
	data_read_array(&pos_views);
	while (! data_read_array_end(&pos_views))
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
