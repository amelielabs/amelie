
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>
#include <amelie_db.h>

void
db_init(Db*        self,
        Cdc*       cdc,
        CatalogIf* iface,
        EvalIf*    iface_eval,
        PartsIf*   iface_parts,
        void*      iface_arg)
{
	self->cdc = cdc;
	catalog_init(&self->catalog, cdc,
	             iface,
	             iface_eval, iface_parts, iface_arg);
	wal_init(&self->wal);
	checkpoints_init(&self->checkpoints, &self->catalog);
	syncer_init(&self->syncer, self);
}

void
db_free(Db* self)
{
	checkpoints_free(&self->checkpoints);
	catalog_free(&self->catalog);
	wal_free(&self->wal);
}

static void
db_bootstrap(Db* self)
{
	// create initial checkpoint
	Checkpoint checkpoint;
	checkpoint_init(&checkpoint, &self->catalog);
	defer(checkpoint_free, &checkpoint);
	checkpoint_begin(&checkpoint, 1, 1);
	checkpoint_run(&checkpoint);
	checkpoint_wait(&checkpoint);

	checkpoints_add(&self->checkpoints, 1);
}

void
db_open(Db* self, bool bootstrap)
{
	state_lsn_set(1);
	state_checkpoint_set(1);

	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	wal_open(&self->wal);

	// create superuser
	catalog_create(&self->catalog);

	// create initial checkpoint
	if (bootstrap)
	{
		db_bootstrap(self);
		return;
	}

	// restore last checkpoint
	checkpoints_open(&self->checkpoints);
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
	encode_str(buf, &config()->version.string);

	// directory
	encode_raw(buf, "directory", 9);
	encode_str(buf, &state()->directory.string);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_uuid(buf, opt_uuid_of(&config()->uuid));

	// frontends
	encode_raw(buf, "frontends", 9);
	encode_int(buf, opt_int_of(&config()->frontends));

	// backends
	encode_raw(buf, "backends", 8);
	encode_int(buf, opt_int_of(&config()->backends));

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_int(buf, state_lsn());

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_int(buf, state_checkpoint());

	encode_obj_end(buf);
}
