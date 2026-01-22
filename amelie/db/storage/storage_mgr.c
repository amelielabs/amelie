
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

void
storage_mgr_init(StorageMgr* self)
{
	relation_mgr_init(&self->mgr);
}

void
storage_mgr_free(StorageMgr* self)
{
	relation_mgr_free(&self->mgr);
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
			error("storage '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// allocate storage and init
	auto storage = storage_allocate(config);

	// register storage
	relation_mgr_create(&self->mgr, tr, &storage->rel);

	// create storage directory, if not exists
	char path[PATH_MAX];
	storage_fmt(storage, path, "", NULL);
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

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
			error("storage '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	if (storage->refs > 0)
		error("storage '%.*s': is being used", str_size(name),
		      str_of(name));

	if (storage->config->system)
		error("storage '%.*s': system storage cannot be dropped", str_size(name),
		      str_of(name));

	// drop storage by object
	relation_mgr_drop(&self->mgr, tr, &storage->rel);
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
	auto relation = log_relation_of(self, op);
	auto mgr = storage_of(relation->relation);
	// set previous name
	uint8_t* pos = relation->data;
	Str name;
	json_read_string(&pos, &name);
	storage_config_set_name(mgr->config, &name);
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
			error("storage '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (storage->refs > 0)
		error("storage '%.*s': is being used", str_size(name),
		      str_of(name));

	if (storage->config->system)
		error("storage '%.*s': system storage cannot be renamed", str_size(name),
		       str_of(name));

	// ensure new storage does not exists
	if (storage_mgr_find(self, name_new, false))
		error("storage '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update storage
	log_relation(&tr->log, &rename_if, NULL, &storage->rel);

	// save name for rollback
	encode_string(&tr->log.data, name);

	// set new name
	storage_config_set_name(storage->config, name_new);
	return true;
}

void
storage_mgr_dump(StorageMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto storage = storage_of(list_at(Relation, link));
		if (storage->config->system)
			continue;
		storage_config_write(storage->config, buf);
	}
	encode_array_end(buf);
}

Storage*
storage_mgr_find(StorageMgr* self, Str* name, bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, NULL, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("storage '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return storage_of(relation);
}

Buf*
storage_mgr_list(StorageMgr* self, Str* name, bool extended)
{
	unused(extended);
	auto buf = buf_create();
	if (name)
	{
		auto storage = storage_mgr_find(self, name, false);
		if (storage)
			storage_config_write(storage->config, buf);
		else
			encode_null(buf);
	} else
	{
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto storage = storage_of(list_at(Relation, link));
			storage_config_write(storage->config, buf);
		}
		encode_array_end(buf);
	}
	return buf;
}
