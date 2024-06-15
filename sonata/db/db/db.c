
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>

void
db_init(Db* self, PartMapper mapper, void* mapper_arg)
{
	table_mgr_init(&self->table_mgr, mapper, mapper_arg);
	view_mgr_init(&self->view_mgr);
	schema_mgr_init(&self->schema_mgr);
	checkpoint_mgr_init(&self->checkpoint_mgr);
	wal_init(&self->wal);
}

void
db_free(Db* self)
{
	table_mgr_free(&self->table_mgr);
	view_mgr_free(&self->view_mgr);
	schema_mgr_free(&self->schema_mgr);
	checkpoint_mgr_free(&self->checkpoint_mgr);
	wal_free(&self->wal);
}

static void
db_create_system_schema(Db* self, const char* schema, bool create)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// begin
		transaction_begin(&trx);

		// create system schema
		Str name;
		str_init(&name);
		str_set_cstr(&name, schema);
		auto config = schema_config_allocate();
		guard(schema_config_free, config);
		schema_config_set_name(config, &name);
		schema_config_set_system(config, true);
		schema_config_set_create(config, create);
		schema_mgr_create(&self->schema_mgr, &trx, config, false);

		// commit
		transaction_commit(&trx);
	}

	if (leave(&e))
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

	// read directory and restore last checkpoint catalog
	// (schemas, tables, views)
	checkpoint_mgr_open(&self->checkpoint_mgr, cat_mgr);
}

void
db_close(Db* self)
{
	// free views
	view_mgr_free(&self->view_mgr);

	// free tables
	table_mgr_free(&self->table_mgr);

	// todo: wal close?
}
