
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

void
db_init(Db* self, MetaIf* iface, void* arg)
{
	storage_mgr_init(&self->storage_mgr);
	table_mgr_init(&self->table_mgr);
	meta_mgr_init(&self->meta_mgr, iface, arg);
	schema_mgr_init(&self->schema_mgr);
	wal_init(&self->wal);
}

void
db_free(Db* self)
{
	storage_mgr_free(&self->storage_mgr);
	table_mgr_free(&self->table_mgr);
	meta_mgr_free(&self->meta_mgr);
	schema_mgr_free(&self->schema_mgr);
	wal_free(&self->wal);
}

static void
db_prepare(Db* self)
{
	Transaction trx;
	transaction_init(&trx);
	guard(trx_guard, transaction_free, &trx);

	Exception e;
	if (try(&e))
	{
		// begin
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

		// create public schema
		Str name;
		str_init(&name);
		str_set_cstr(&name, "public");
		auto config = schema_config_allocate();
		guard(config_guard, schema_config_free, config);
		schema_config_set_name(config, &name);
		schema_config_set_system(config, true);
		schema_mgr_create(&self->schema_mgr, &trx, config, false);

		// commit
		transaction_commit(&trx);
	}

	if (catch(&e))
	{
		transaction_abort(&trx);
		rethrow();
	}
}

void
db_open(Db* self, CatalogMgr* cat_mgr)
{
	// prepare system objects
	db_prepare(self);

	// recover storages
	storage_mgr_open(&self->storage_mgr);

	// register catalog
	auto cat = catalog_allocate("db", &db_catalog_if, self);
	catalog_mgr_add(cat_mgr, cat);
}

void
db_close(Db* self)
{
	// stop wal
	wal_stop(&self->wal);

	// free meta
	meta_mgr_free(&self->meta_mgr);

	// free tables
	table_mgr_free(&self->table_mgr);

	// stop storages
	storage_mgr_free(&self->storage_mgr);
}
