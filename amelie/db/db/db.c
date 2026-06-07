
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
#include <amelie_cdc.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>
#include <amelie_db.h>

void
db_init(Db*        self,
        CatalogIf* iface,
        void*      iface_arg,
        PartMgrIf* iface_part_mgr,
        void*      iface_part_mgr_arg,
        Cdc*       cdc)
{
	self->snapshots_count = 0;
	self->cdc             = cdc;
	catalog_init(&self->catalog, iface, iface_arg,
	             iface_part_mgr,
	             iface_part_mgr_arg,
	             cdc);
	wal_init(&self->wal);
	list_init(&self->snapshots);
	list_init(&self->snapshots_gc);
	checkpoint_mgr_init(&self->checkpoint_mgr, &self->catalog);
	syncer_init(&self->syncer, self);
}

void
db_free(Db* self)
{
	assert(! self->snapshots_count);
	checkpoint_mgr_free(&self->checkpoint_mgr);
	catalog_free(&self->catalog);
	wal_free(&self->wal);
}

static void
db_bootstrap(Db* self)
{
	// first valid transaction id starts from 1
	state_tsn_set(0);
	state_lsn_set(1);
	state_checkpoint_set(1);

	// create system objects
	catalog_create(&self->catalog);

	// create initial checkpoint
	Checkpoint checkpoint;
	checkpoint_init(&checkpoint, &self->catalog);
	defer(checkpoint_free, &checkpoint);
	checkpoint_begin(&checkpoint, 1, 1);
	checkpoint_run(&checkpoint);
	checkpoint_wait(&checkpoint);
}

void
db_open(Db* self, bool bootstrap)
{
	// create initial checkpoint
	if (bootstrap)
	{
		db_bootstrap(self);
		return;
	}

	// restore last checkpoint
	auto cp_mgr = &self->checkpoint_mgr;
	checkpoint_mgr_open(cp_mgr);
}

void
db_close(Db* self)
{
	// stop syncer
	syncer_stop(&self->syncer);

	// stop wal
	wal_close(&self->wal);
}

void
db_state(Db* self, Buf* buf)
{
	unused(self);

	// {}
	encode_obj(buf);

	// version
	encode_raw(buf, "version", 7);
	encode_str(buf, &state()->version.string);

	// directory
	encode_raw(buf, "directory", 9);
	encode_str(buf, &state()->directory.string);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_str(buf, &config()->uuid.string);

	// frontends
	encode_raw(buf, "frontends", 9);
	encode_int(buf, opt_int_of(&config()->frontends));

	// backends
	encode_raw(buf, "backends", 8);
	encode_int(buf, opt_int_of(&config()->backends));

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_int(buf, state_lsn());

	// tsn
	encode_raw(buf, "tsn", 3);
	encode_int(buf, state_tsn());

	// psn
	encode_raw(buf, "psn", 3);
	encode_int(buf, state_psn());

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_int(buf, state_checkpoint());

	// read_only
	encode_raw(buf, "read_only", 9);
	encode_bool(buf, opt_int_of(&state()->read_only));

	encode_obj_end(buf);
}
