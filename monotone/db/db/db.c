
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

void
db_init(Db*        self,
        MetaIface* meta_iface,
        void*      meta_iface_arg)
{
	table_mgr_init(&self->table_mgr, &self->compact_mgr);
	meta_mgr_init(&self->meta_mgr, meta_iface, meta_iface_arg);
	wal_init(&self->wal);
	compact_mgr_init(&self->compact_mgr);
}

void
db_free(Db* self)
{
	wal_free(&self->wal);
}

void
db_open(Db* self, CatalogMgr* cat_mgr)
{
	// start workers
	compact_mgr_start(&self->compact_mgr, var_int_of(&config()->engine_workers));

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

	// stop workers
	compact_mgr_stop(&self->compact_mgr);
}

void
db_checkpoint(Db* self)
{
	(void)self;
#if 0
	uint64_t snapshot_min;
	snapshot_min = id_mgr_min(&self->mvcc->snapshot_mgr);

	// metas
	meta_mgr_gc(&self->meta_mgr, snapshot_min);

	// tables
	table_mgr_gc(&self->table_mgr, snapshot_min);
#endif
}
