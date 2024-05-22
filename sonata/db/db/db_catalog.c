
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>

static void
db_catalog_dump(Catalog* cat, Buf* buf)
{
	// { schemas, tables, views }
	Db* self = cat->iface_arg;
	encode_map(buf, 3);
	encode_raw(buf, "schemas", 7);
	schema_mgr_dump(&self->schema_mgr, buf);
	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);
	encode_raw(buf, "views", 5);
	view_mgr_dump(&self->view_mgr, buf);
}

static void
db_catalog_restore_schema(Db* self, uint64_t lsn, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

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

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	transaction_commit(&trx);
}

static void
db_catalog_restore_table(Db* self, uint64_t lsn, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

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

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	transaction_commit(&trx);
}

static void
db_catalog_restore_view(Db* self, uint64_t lsn, uint8_t** pos)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

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

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	transaction_commit(&trx);
}

static void
db_catalog_restore(Catalog* cat, uint64_t lsn, uint8_t** pos)
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
	int count;
	data_read_array(&pos_schemas, &count);
	int i = 0;
	for (; i < count; i++)
		db_catalog_restore_schema(self, lsn, &pos_schemas);

	// tables
	data_read_array(&pos_tables, &count);
	for (i = 0; i < count; i++)
		db_catalog_restore_table(self, lsn, &pos_tables);

	// views
	data_read_array(&pos_views, &count);
	for (i = 0; i < count; i++)
		db_catalog_restore_view(self, lsn, &pos_views);
}

CatalogIf db_catalog_if =
{
	.dump    = db_catalog_dump,
	.restore = db_catalog_restore
};
