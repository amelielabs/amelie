
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

static void
db_catalog_dump(Catalog* cat, Buf* buf)
{
	// { schemas, tables, metas }
	Db* self = cat->iface_arg;
	encode_map(buf, 3);
	encode_raw(buf, "schemas", 7);
	schema_mgr_dump(&self->schema_mgr, buf);
	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);
	encode_raw(buf, "metas", 5);
	meta_mgr_dump(&self->meta_mgr, buf);
}

static void
db_catalog_restore_schema(Db* self, uint64_t lsn, uint8_t** pos)
{
	// [table_config]
	int count;
	data_read_array(pos, &count);

	Transaction trx;
	transaction_init(&trx);
	guard(trx_guard, transaction_free, &trx);

	Exception e;
	if (try(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

		// read schema config
		auto config = schema_config_read(pos);
		guard(config_guard, schema_config_free, config);

		// create schema
		schema_mgr_create(&self->schema_mgr, &trx, config, false);
	}

	if (catch(&e))
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
	// [table_config]
	int count;
	data_read_array(pos, &count);

	Transaction trx;
	transaction_init(&trx);
	guard(trx_guard, transaction_free, &trx);

	Exception e;
	if (try(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

		// read table config
		auto config = table_config_read(pos);
		guard(config_guard, table_config_free, config);

		// create table
		table_mgr_create(&self->table_mgr, &trx, config, false);
	}

	if (catch(&e))
	{
		transaction_abort(&trx);
		rethrow();
	}

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	transaction_commit(&trx);
}

static void
db_catalog_restore_meta(Db* self, uint64_t lsn, uint8_t** pos)
{
	// [meta_config]
	int count;
	data_read_array(pos, &count);

	Transaction trx;
	transaction_init(&trx);
	guard(trx_guard, transaction_free, &trx);

	Exception e;
	if (try(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

		// read meta config
		auto config = meta_config_read(pos);
		guard(config_guard, meta_config_free, config);

		// create meta
		meta_mgr_create(&self->meta_mgr, &trx, config, false);
	}

	if (catch(&e))
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
	// { tables, metas }
	Db* self = cat->iface_arg;

	int count;
	data_read_map(pos, &count);

	// schemas
	data_skip(pos);
	data_read_array(pos, &count);
	int i = 0;
	for (; i < count; i++)
		db_catalog_restore_schema(self, lsn, pos);

	// tables
	data_skip(pos);
	data_read_array(pos, &count);
	for (i = 0; i < count; i++)
		db_catalog_restore_table(self, lsn, pos);

	// metas
	data_skip(pos);
	data_read_array(pos, &count);
	for (i = 0; i < count; i++)
		db_catalog_restore_meta(self, lsn, pos);
}

CatalogIf db_catalog_if =
{
	.dump    = db_catalog_dump,
	.restore = db_catalog_restore
};
