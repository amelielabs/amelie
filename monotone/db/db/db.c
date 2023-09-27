
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
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_db.h>

void
db_init(Db* self, MetaIf* iface, void* arg)
{
	storage_mgr_init(&self->storage_mgr);
	table_mgr_init(&self->table_mgr);
	meta_mgr_init(&self->meta_mgr, iface, arg);
}

void
db_free(Db* self)
{
	storage_mgr_free(&self->storage_mgr);
	table_mgr_free(&self->table_mgr);
	meta_mgr_free(&self->meta_mgr);
}

void
db_open(Db* self, CatalogMgr* cat_mgr)
{
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

	// free meta
	meta_mgr_free(&self->meta_mgr);

	// free tables
	table_mgr_free(&self->table_mgr);

	// stop storages
	storage_mgr_free(&self->storage_mgr);
}
