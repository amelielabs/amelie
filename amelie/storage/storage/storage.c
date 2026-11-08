
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
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
storage_init(Storage*   self,
             CatalogIf* iface,
             EvalIf*    iface_eval,
             void*      iface_arg)
{
	unused(iface_eval);
	catalog_init(&self->users, iface, iface_arg, NULL, NULL);
	wal_init(&self->wal);
	syncer_init(&self->syncer, self);
}

void
storage_free(Storage* self)
{
	catalog_free(&self->users);
	wal_free(&self->wal);
}

static void
storage_bootstrap(Storage* self)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// create amelie user
		auto user_config = user_config_allocate();
		defer(user_config_free, user_config);
		Str name;
		str_init(&name);
		str_set_cstr(&name, "amelie");
		user_config_set_name(user_config, &name);
		user_config_set_parent(user_config, &name);
		user_config_set_superuser(user_config, true);
		user_create(&self->users, &tr, user_config, false);

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
storage_open(Storage* self, bool bootstrap)
{
	state_lsn_set(1);
	state_checkpoint_set(0);

	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	wal_open(&self->wal);

	// create superuser
	storage_bootstrap(self);

	// create initial checkpoint
	if (bootstrap)
	{
		// create initial checkpoint
		// storage_checkpoint(self);
		return;
	}

	// todo: restore last checkpoint
}

void
storage_close(Storage* self)
{
	// stop syncer
	syncer_stop(&self->syncer);

	// stop wal
	wal_close(&self->wal);
}

void
storage_state(Storage* self, Buf* buf)
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
