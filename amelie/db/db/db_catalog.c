
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

static void
db_catalog_dump(Catalog* cat, Buf* buf)
{
	// { schemas, tables, views }
	Db* self = cat->iface_arg;

	encode_map(buf);

	encode_raw(buf, "schemas", 7);
	schema_mgr_dump(&self->schema_mgr, buf);

	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	encode_raw(buf, "views", 5);
	view_mgr_dump(&self->view_mgr, buf);

	encode_map_end(buf);
}

static void
db_catalog_restore_schema(Db* self, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);

		// read schema config
		auto config = schema_config_read(pos);
		guard(schema_config_free, config);

		// create schema
		schema_mgr_create(&self->schema_mgr, &trx, config, false);
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
db_catalog_restore_table(Db* self, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);

		// read table config
		auto config = table_config_read(pos);
		guard(table_config_free, config);

		// create table
		table_mgr_create(&self->table_mgr, &trx, config, false);
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
db_catalog_restore_view(Db* self, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);

		// read view config
		auto config = view_config_read(pos);
		guard(view_config_free, config);

		// create view
		view_mgr_create(&self->view_mgr, &trx, config, false);
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
db_catalog_restore(Catalog* cat, uint8_t** pos)
{
	// { schemas, tables, views }
	Db* self = cat->iface_arg;

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
	decode_map(map, pos);

	// schemas
	data_read_array(&pos_schemas);
	while (! data_read_array_end(&pos_schemas))
		db_catalog_restore_schema(self, &pos_schemas);

	// tables
	data_read_array(&pos_tables);
	while (! data_read_array_end(&pos_tables))
		db_catalog_restore_table(self, &pos_tables);

	// views
	data_read_array(&pos_views);
	while (! data_read_array_end(&pos_views))
		db_catalog_restore_view(self, &pos_views);
}

CatalogIf db_catalog_if =
{
	.dump    = db_catalog_dump,
	.restore = db_catalog_restore
};
