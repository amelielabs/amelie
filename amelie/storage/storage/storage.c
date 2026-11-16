
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
	self->iface       = iface_eval;
	self->iface_arg   = iface_arg;
	self->catalog_lsn = 0;
	catalog_init(&self->catalog, iface, iface_arg, NULL, NULL);
	wal_init(&self->wal);
	syncer_init(&self->syncer, self);
}

void
storage_free(Storage* self)
{
	catalog_free(&self->catalog);
	wal_free(&self->wal);
}

static Db*
storage_bootstrap(Storage* self)
{
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);

	// create db
	auto config = db_config_allocate();
	defer(db_config_free, config);

	Str name;
	str_init(&name);
	str_set_cstr(&name, "amelie");
	db_config_set_name(config, &name);
	db_create(&self->catalog, &tr, config, false);

	// commit
	tr_commit(&tr);
	return catalog_find_db(&self->catalog, &name, true);
}

static inline int
storage_recover_id_of(const char* name, uint64_t* lsn,  bool* incomplete)
{
	// <lsn>[.incomplete]
	*lsn = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(uint64_mul_add_overflow(lsn, *lsn, 10, *name - '0')))
			return -1;
		name++;
	}
	if (! *name)
	{
		*incomplete = false;
		return 0;
	}
	*incomplete = !strcmp(name, ".incomplete");
	if (*incomplete)
		return 0;
	return -1;
}

static void
storage_recover_catalog(Storage* self)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/storage", state_directory());

	// find latest catalog file
	uint64_t lsn_catalog = 0;

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

		// read file name
		bool incomplete = false;
		uint64_t lsn;
		if (storage_recover_id_of(entry->d_name, &lsn, &incomplete) == -1)
			continue;

		// remove incomplete file
		if (incomplete)
		{
			info("storage: removing incomplete catalog file: '{s}/{s}'",
			     path, entry->d_name);
			fs_unlink_if_exists("{s}/{u64}.incomplete", path, lsn);
			continue;
		}

		// track last version
		if (lsn_catalog < lsn)
			lsn_catalog = lsn;
	}

	if (! lsn_catalog)
		error("storage: catalog file is missing");

	// read file
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);
	file_import_stream(&sep.buf, "{s}/storage/{u64}", state_directory(),
	                   lsn_catalog);

	// prepare eval
	Eval eval;
	eval_init(&eval, self->iface, self->iface_arg);
	eval_create(&eval);
	defer(eval_free, &eval);

	// parse and execute statements
	Str db;
	str_init(&db);
	str_set_cstr(&db, "amelie");
	Str command;
	while (separator_read(&sep, &command))
	{
		eval_execute(&eval, &db, &command);
		separator_advance(&sep);
	}

	// set lsn
	self->catalog_lsn = lsn_catalog;
}

static void
storage_recover(Storage* self)
{
	// read catalog and recreate databases
	storage_recover_catalog(self);

	// open databases
	RpcSet set;
	rpc_set_init(&set);
	defer(rpc_set_free, &set);
	rpc_set_prepare(&set, self->catalog.rels.list_count);

	auto order = 0;
	list_foreach(&self->catalog.rels.list)
	{
		auto db = db_of(list_at(Rel, link));
		auto rpc = rpc_set_add(&set, order, MSG_OPEN, db);
		rpc_send(rpc, db->compute);
		order++;
	}

	rpc_set_wait(&set);
}

void
storage_start(Storage* self, bool bootstrap)
{
	state_lsn_set(1);
	state_checkpoint_set(0);

	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	wal_open(&self->wal);

	// create system db
	storage_bootstrap(self);

	if (bootstrap)
	{
		// create initial checkpoint
		storage_checkpoint_catalog(self);
		return;
	}

	// read and restore databases
	storage_recover(self);
}

void
storage_stop(Storage* self)
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
