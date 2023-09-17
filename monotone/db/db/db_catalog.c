
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
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

static void
db_catalog_dump_tables(Db* self, Buf* buf)
{
	// [[table_config]]
	int count = 0;
	int array_offset = buf_size(buf);
	encode_array32(buf, 0);

	list_foreach(&self->table_mgr.tables.list)
	{
		auto handle = list_at(Handle, link);
		if (handle->drop)
			continue;
		auto table = table_of(handle);
		encode_array(buf, 1);
		table_config_write(table->config, buf);
		count++;
	}

	uint8_t* array_start = buf->start + array_offset;
	data_write_array32(&array_start, count);
}

static void
db_catalog_dump_metas(Db* self, Buf* buf)
{
	// [[meta_config]]
	int count = 0;
	int array_offset = buf_size(buf);
	encode_array32(buf, 0);

	list_foreach(&self->meta_mgr.metas.list)
	{
		auto handle = list_at(Handle, link);
		if (handle->drop)
			continue;
		auto meta = meta_of(handle);
		encode_array(buf, 1);
		meta_config_write(meta->config, buf);
		count++;
	}

	uint8_t* array_start = buf->start + array_offset;
	data_write_array32(&array_start, count);
}

static void
db_catalog_dump(Catalog* cat, Buf* buf)
{
	// { tables, metas }
	Db* self = cat->iface_arg;
	encode_map(buf, 2);
	encode_raw(buf, "tables", 6);
	db_catalog_dump_tables(self, buf);
	encode_raw(buf, "metas", 5);
	db_catalog_dump_metas(self, buf);
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
		mvcc_begin(&trx);
		transaction_set_auto_commit(&trx);

		// read table config
		auto config = table_config_read(pos);
		guard(config_guard, table_config_free, config);

		// create table
		table_mgr_create(&self->table_mgr, &trx, config, false);
	}

	if (catch(&e))
	{
		mvcc_abort(&trx);
		rethrow();
	}

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	mvcc_commit(&trx);
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
		mvcc_begin(&trx);
		transaction_set_auto_commit(&trx);

		// read table config
		auto config = meta_config_read(pos);
		guard(config_guard, meta_config_free, config);

		// create meta
		meta_mgr_create(&self->meta_mgr, &trx, config, false);
	}

	if (catch(&e))
	{
		mvcc_abort(&trx);
		rethrow();
	}

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	mvcc_commit(&trx);
}

static void
db_catalog_restore(Catalog* cat, uint64_t lsn, uint8_t** pos)
{
	// { tables, metas }
	Db* self = cat->iface_arg;

	int count;
	data_read_map(pos, &count);

	// tables
	data_skip(pos);
	data_read_array(pos, &count);
	int i = 0;
	for (; i < count; i++)
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
