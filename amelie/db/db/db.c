
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
db_open(Db* self)
{
	// prepare system schemas
	db_create_system_schema(self, "system", false);
	db_create_system_schema(self, "public", true);

	// read directory and restore last checkpoint catalog
	// (schemas, tables, views)
	Catalog catalog;
	catalog_init(&catalog, &db_catalog_if,  self);
	checkpoint_mgr_open(&self->checkpoint_mgr, &catalog);
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
