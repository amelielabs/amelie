
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
#include <amelie_index.h>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
db_init(Db*        self,
        CatalogIf* iface,
        EvalIf*    iface_eval,
        void*      iface_arg)
{
	unused(iface_eval);

	catalog_init(&self->databases, iface, iface_arg, NULL, NULL);
	wal_init(&self->wal);
	syncer_init(&self->syncer, self);
}

void
db_free(Db* self)
{
	catalog_free(&self->databases);
	wal_free(&self->wal);
}

static void
db_bootstrap(Db* self)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// create amelie database
		auto config = database_config_allocate();
		defer(database_config_free, config);
		Str name;
		str_init(&name);
		str_set_cstr(&name, "amelie");
		database_config_set_name(config, &name);
		database_config_set_parent(config, &name);
		database_config_set_system(config, true);
		database_create(&self->databases, &tr, config, false);

		// commit
		tr_commit(&tr);
	);
	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}

void
db_open(Db* self, bool bootstrap)
{
	state_lsn_set(1);
	state_checkpoint_set(0);

	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	wal_open(&self->wal);

	// create superdatabase
	db_bootstrap(self);

	// create initial checkpoint
	if (bootstrap)
	{
		// create initial checkpoint
		// db_checkpoint(self);
		return;
	}

	// todo: restore last checkpoint
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

	// compute
	encode_raw(buf, "compute", 7);
	encode_int(buf, opt_int_of(&config()->compute));

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_int(buf, state_lsn());

	// psn
	encode_raw(buf, "psn", 3);
	encode_int(buf, state_psn());

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_int(buf, state_checkpoint());

	encode_obj_end(buf);
}
