
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
	catalog_init(&self->databases, iface, iface_arg, NULL, NULL);
	wal_init(&self->wal);
	syncer_init(&self->syncer, self);
}

void
storage_free(Storage* self)
{
	catalog_free(&self->databases);
	wal_free(&self->wal);
}

#if 0
static void
checkpoints_open_schema(Checkpoints* self, Manifest* manifest)
{
	// restore last checkpoint schema
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/storage/{u64}/schema.sql",
	       state_directory(), state_checkpoint());

	// read file
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);
	file_import_stream(&sep.buf, "{s}", path);

	// prepare eval
	auto catalog = self->catalog;
	Eval eval;
	eval_init(&eval, catalog->iface_eval, catalog->iface_arg);
	eval_create(&eval);
	defer(eval_free, &eval);

	// set manifest used during recovery
	catalog->manifest = manifest;

	// parse and execute statements
	Str command;
	while (separator_read(&sep, &command))
	{
		eval_execute(&eval, &command);
		separator_advance(&sep);
	}

	catalog->manifest = NULL;
}
#endif


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
		// create amelie db
		auto config = db_config_allocate();
		defer(db_config_free, config);
		Str name;
		str_init(&name);
		str_set_cstr(&name, "amelie");
		db_config_set_name(config, &name);
		db_config_set_system(config, true);
		db_create(&self->databases, &tr, config, false);

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

	// create system db
	storage_bootstrap(self);

	// todo: restore databases
	(void)bootstrap;
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

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_int(buf, state_checkpoint());

	encode_obj_end(buf);
}
