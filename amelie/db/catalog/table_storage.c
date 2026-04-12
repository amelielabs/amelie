
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
storage_add_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
storage_add_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str storage_name;
	json_read_string(&pos, &storage_name);

	Table* table = op->iface_arg;
	auto volumes = &table->config->partitioning.volumes;
	auto volume = volume_mgr_find(volumes, &storage_name);
	assert(volume);
	error_catch (
		volume_rmdir(volume);
	);
	storage_unref(volume->storage);
	volume_mgr_remove(volumes, volume);
	volume_free(volume);
}

static LogIf storage_add_if =
{
	.commit = storage_add_if_commit,
	.abort  = storage_add_if_abort
};

bool
table_storage_add(Table*  self,
                  Tr*     tr,
                  Volume* config,
                  bool    if_not_exists)
{
	// only owner or superuser
	check_ownership(tr, &self->rel);

	// find storage
	auto storage = storage_mgr_find(self->part_mgr.storage_mgr, &config->name, true);

	// ensure storage not exists
	auto volumes = &self->config->partitioning.volumes;
	auto volume = volume_mgr_find(volumes, &config->name);
	if (volume)
	{
		if (! if_not_exists)
			error("table '%.*s' storage '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// update table
	log_rel(&tr->log, &storage_add_if, self, &self->rel);

	// save storage name
	encode_string(&tr->log.data, &config->name);

	// add volume to the storage
	volume = volume_copy(config);
	volume->storage = storage;
	storage_ref(storage);
	volume_mgr_add(volumes, volume);

	// create directory
	volume_mkdir(volume);
	return true;
}

static void
storage_drop_if_commit(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str storage_name;
	json_read_string(&pos, &storage_name);

	Table* table = op->iface_arg;
	auto volumes = &table->config->partitioning.volumes;
	auto volume = volume_mgr_find(volumes, &storage_name);
	assert(volume);
	error_catch (
		volume_rmdir(volume);
	);
	storage_unref(volume->storage);
	volume_mgr_remove(volumes, volume);
	volume_free(volume);
}

static void
storage_drop_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf storage_drop_if =
{
	.commit = storage_drop_if_commit,
	.abort  = storage_drop_if_abort
};

bool
table_storage_drop(Table* self,
                   Tr*    tr,
                   Str*   name,
                   bool   if_exists)
{
	// only owner or superuser
	check_ownership(tr, &self->rel);

	// ensure volume exists
	auto volumes = &self->config->partitioning.volumes;
	auto volume = volume_mgr_find(volumes, name);
	if (! volume)
	{
		if (! if_exists)
			error("table '%.*s' storage '%.*s': not found",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// ensure volume has no deps
	if (volume->list_count > 0)
		error("table '%.*s' storage '%.*s': is not empty",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

	// update table
	log_rel(&tr->log, &storage_drop_if, self, &self->rel);

	// save storage name
	encode_string(&tr->log.data, name);
	return true;
}

static void
storage_pause_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
storage_pause_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str storage_name;
	json_read_string(&pos, &storage_name);

	Table* table = op->iface_arg;
	auto volumes = &table->config->partitioning.volumes;
	auto volume = volume_mgr_find(volumes, &storage_name);
	assert(volume);
	volume_set_pause(volume, !volume->pause);
}

static LogIf storage_pause_if =
{
	.commit = storage_pause_if_commit,
	.abort  = storage_pause_if_abort
};

bool
table_storage_pause(Table* self,
                    Tr*    tr,
                    Str*   name,
                    bool   pause,
                    bool   if_exists)
{
	// only owner or superuser
	check_ownership(tr, &self->rel);

	// ensure storage exists
	auto volumes = &self->config->partitioning.volumes;
	auto volume = volume_mgr_find(volumes, name);
	if (! volume)
	{
		if (! if_exists)
			error("table '%.*s' storage '%.*s': not found",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	if (volume->pause == pause)
		return false;

	// ensure at least one volume is still active
	if (pause && volume_mgr_count(volumes) <= 1)
		error("table '%.*s': at least one storage must remain active",
		      str_size(&self->config->name),
		      str_of(&self->config->name));

	// update table
	log_rel(&tr->log, &storage_pause_if, self, &self->rel);

	// apply
	volume_set_pause(volume, pause);

	// save storage name
	encode_string(&tr->log.data, name);
	return true;
}
