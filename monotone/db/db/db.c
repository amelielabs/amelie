
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>

void
db_init(Db* self)
{
	table_mgr_init(&self->table_mgr);
	view_mgr_init(&self->view_mgr);
	schema_mgr_init(&self->schema_mgr);
	wal_init(&self->wal);
}

void
db_free(Db* self)
{
	table_mgr_free(&self->table_mgr);
	view_mgr_free(&self->view_mgr);
	schema_mgr_free(&self->schema_mgr);
	wal_free(&self->wal);
}

static void
db_create_system_schema(Db* self, const char* schema, bool create)
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

		// create system schema
		Str name;
		str_init(&name);
		str_set_cstr(&name, schema);
		auto config = schema_config_allocate();
		guard(config_guard, schema_config_free, config);
		schema_config_set_name(config, &name);
		schema_config_set_system(config, true);
		schema_config_set_create(config, create);
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
	// prepare system schemas
	db_create_system_schema(self, "system", false);
	db_create_system_schema(self, "public", true);

	// register db catalog
	auto cat = catalog_allocate("db", &db_catalog_if, self);
	catalog_mgr_add(cat_mgr, cat);
}

void
db_close(Db* self)
{
	// free views
	view_mgr_free(&self->view_mgr);

	// free tables
	table_mgr_free(&self->table_mgr);

	// stop wal
	wal_stop(&self->wal);
}
