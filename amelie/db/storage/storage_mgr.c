
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
#include <amelie_storage.h>

void
storage_mgr_init(StorageMgr* self)
{
	rel_mgr_init(&self->mgr);
}

void
storage_mgr_free(StorageMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
storage_mgr_create(StorageMgr*    self,
                   Tr*            tr,
                   StorageConfig* config,
                   bool           if_not_exists)
{
	// make sure storage does not exists
	auto current = storage_mgr_find(self, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("storage '{str}': already exists", &config->name);
		return false;
	}

	// allocate storage and init
	auto storage = storage_allocate(config);

	// register storage
	rel_mgr_create(&self->mgr, tr, &storage->rel);
	return true;
}

bool
storage_mgr_drop(StorageMgr* self,
                 Tr*         tr,
                 Str*        name,
                 bool        if_exists)
{
	auto storage = storage_mgr_find(self, name, false);
	if (! storage)
	{
		if (! if_exists)
			error("storage '{str}': not exists", name);
		return false;
	}
	if (storage->refs > 0)
		error("storage '{str}': is being used", name);

	if (storage->config->system)
		error("storage '{str}': system storage cannot be dropped", name);

	// drop storage by object
	rel_mgr_drop(&self->mgr, tr, &storage->rel);
	return true;
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	// set previous name
	uint8_t* pos = log_data_of(self, op);
	Str name;
	unpack_str(&pos, &name);

	StorageMgr* mgr = op->iface_arg;
	rel_mgr_rename(&mgr->mgr, op->rel, NULL, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
storage_mgr_rename(StorageMgr* self,
                   Tr*         tr,
                   Str*        name,
                   Str*        name_new,
                   bool        if_exists)
{
	auto storage = storage_mgr_find(self, name, false);
	if (! storage)
	{
		if (! if_exists)
			error("storage '{str}': not exists", name);
		return false;
	}

	if (storage->refs > 0)
		error("storage '{str}': is being used", name);

	if (storage->config->system)
		error("storage '{str}': system storage cannot be renamed", name);

	// ensure new storage does not exists
	if (storage_mgr_find(self, name_new, false))
		error("storage '{str}': already exists", name_new);

	// update storage
	log_ddl(&tr->log, &rename_if, self, &storage->rel);

	// save name for rollback
	encode_str(&tr->log.data, name);

	// set new name
	rel_mgr_rename(&self->mgr, &storage->rel, NULL, name_new);
	return true;
}

void
storage_mgr_dump(StorageMgr* self, Buf* buf)
{
	rel_mgr_dump(&self->mgr, REL_STORAGE, buf, 0);
}

Storage*
storage_mgr_find(StorageMgr* self, Str* name, bool error_if_not_exists)
{
	auto rel = rel_mgr_find(&self->mgr, REL_STORAGE, NULL, name, error_if_not_exists);
	return storage_of(rel);
}

void
storage_mgr_list(StorageMgr* self, Buf* buf, Str* name, int flags)
{
	rel_mgr_list(&self->mgr, REL_STORAGE, buf, NULL, name, flags);
}
