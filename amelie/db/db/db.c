
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
db_init(Db*        self,
        CatalogIf* iface,
        void*      iface_arg,
        PartMgrIf* iface_part_mgr,
        void*      iface_part_mgr_arg)
{
	ops_init(&self->ops);
	catalog_init(&self->catalog, iface, iface_arg,
	             iface_part_mgr,
	             iface_part_mgr_arg);
	wal_mgr_init(&self->wal_mgr);
	snapshot_mgr_init(&self->snapshot_mgr, &self->catalog, &self->wal_mgr.wal);
}

void
db_free(Db* self)
{
	catalog_free(&self->catalog);
	wal_mgr_free(&self->wal_mgr);
	snapshot_mgr_free(&self->snapshot_mgr);
	ops_free(&self->ops);
}

void
db_open(Db* self, bool bootstrap)
{
	// do compaction crash recovery
	service_mgr_open();

	// prepare system catalog
	catalog_open(&self->catalog, bootstrap);
}

void
db_close(Db* self)
{
	// close catalog
	catalog_close(&self->catalog);

	// stop wal mgr
	wal_mgr_stop(&self->wal_mgr);
}

Buf*
db_state(Db* self)
{
	unused(self);

	// {}
	auto buf = buf_create();
	encode_obj(buf);

	// version
	encode_raw(buf, "version", 7);
	encode_string(buf, &state()->version.string);

	// directory
	encode_raw(buf, "directory", 9);
	encode_string(buf, &state()->directory.string);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_string(buf, &config()->uuid.string);

	// frontends
	encode_raw(buf, "frontends", 9);
	encode_integer(buf, opt_int_of(&config()->frontends));

	// backends
	encode_raw(buf, "backends", 8);
	encode_integer(buf, opt_int_of(&config()->backends));

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, state_lsn());

	// psn
	encode_raw(buf, "psn", 3);
	encode_integer(buf, state_psn());

	// catalog
	encode_raw(buf, "catalog", 7);
	encode_integer(buf, state_catalog());

	// catalog_pending
	encode_raw(buf, "catalog_pending", 15);
	encode_integer(buf, opt_int_of(&state()->catalog_pending));

	// read_only
	encode_raw(buf, "read_only", 9);
	encode_bool(buf, opt_int_of(&state()->read_only));

	encode_obj_end(buf);
	return buf;
}
