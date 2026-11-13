
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
	self->iface     = iface_eval;
	self->iface_arg = iface_arg;
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

static Db*
storage_create(Storage* self, Uuid* id, Str* name)
{
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);

	// create amelie db
	auto config = db_config_allocate();
	defer(db_config_free, config);
	db_config_set_id(config, id);
	if (name) {
		db_config_set_name(config, name);
	} else
	{
		// use temporary name during recovery
		Str str;
		str_init(&str);
		str_set(&str, "current_recovery", 16);
		db_config_set_name(config, &str);
	}
	db_create(&self->databases, &tr, config, false);

	// commit
	tr_commit(&tr);
	return (Db*)catalog_find_by(&self->databases, REL_DB, id, true);
}

static void
storage_recover_db(Storage* self, Uuid* id)
{
	// recreate db (start with temprorary name)
	auto db = storage_create(self, id, NULL);

	// read database directory
	auto cp = &db->checkpoints;
	checkpoints_open(cp, UINT64_MAX);
	if (! cp->current)
	{
		error("");
		return;
	}

	// restore last db checkpoint schema
	char uuid[UUID_SZ];
	uuid_get(id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/storage/{s}/{u64}.sql",
	       state_directory(), uuid, cp->current->id);

	// read file
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);
	file_import_stream(&sep.buf, "{s}", path);

	// prepare eval
	Eval eval;
	eval_init(&eval, self->iface, self->iface_arg);
	eval_create(&eval);
	defer(eval_free, &eval);

	// parse and execute statements
	Str command;
	while (separator_read(&sep, &command))
	{
		eval_execute(&eval, &db->config->name, &command);
		separator_advance(&sep);
	}
}

static void
storage_recover(Storage* self)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/storage", state_directory());

	// read databases
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("storage: directory '{s}' open error", path);
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;

		// uuid
		Str str;
		str_init(&str);
		str_set_cstr(&str, entry->d_name);
		Uuid id;
		if (uuid_set_nothrow(&id, &str) == -1)
		{
			info("storage: invalid database directory '{s}/{str}'",
			     path, &str);
			continue;
		}

		// restore
		storage_recover_db(self, &id);
	}

	// todo: deploy
}

void
storage_open(Storage* self, bool bootstrap)
{
	state_lsn_set(1);
	state_checkpoint_set(0);

	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	wal_open(&self->wal);

	// create system database once
	if (bootstrap)
	{
		Str name;
		str_init(&name);
		str_set_cstr(&name, "amelie");
		auto amelie = storage_create(self, &config()->uuid.uuid, &name);
		storage_checkpoint(self, amelie);
		return;
	}

	// read and restore databases
	storage_recover(self);
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
