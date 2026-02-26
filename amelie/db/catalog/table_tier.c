
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
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
table_tier_delete(Table* table, Tier* tier)
{
	// remove and free tier
	auto config = tier->config;

	// delete objects and volumes
	error_catch (
		tier_drop(tier);
	);
	tier_mgr_remove(&table->tier_mgr, tier);

	// remove tier from the table config
	table_config_tier_remove(table->config, config);
	tier_config_free(config);
}

static void
create_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
create_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	Tier* tier = op->iface_arg;
	table_tier_delete(table, tier);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

bool
table_tier_create(Table*      self,
                  Tr*         tr,
                  TierConfig* config,
                  bool        if_not_exists)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, &config->name);
	if (tier)
	{
		if (! if_not_exists)
			error("table '%.*s' tier '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(&tier->config->name),
			      str_of(&tier->config->name));
		return false;
	}

	// save tier copy to the table config
	auto config_copy = tier_config_copy(config);
	table_config_tier_add(self->config, config_copy);

	// create new tier
	tier = tier_mgr_create(&self->tier_mgr, config_copy);

	// update table
	log_relation(&tr->log, &create_if, tier, &self->rel);

	// resolve storages and recover tiers
	tier_recover(tier, self->tier_mgr.storage_mgr);

	// create volumes directories
	volume_mgr_mkdir(&config_copy->volumes);
	return true;
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	Tier* tier = op->iface_arg;
	table_tier_delete(table, tier);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

bool
table_tier_drop(Table* self,
                Tr*    tr,
                Str*   name,
                bool   if_exists)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, name);
	if (! tier)
	{
		if (! if_exists)
			error("table '%.*s' tier '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// update table
	log_relation(&tr->log, &drop_if, tier, &self->rel);
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
	Tier* tier = op->iface_arg;
	auto relation = log_relation_of(self, op);
	uint8_t* pos = relation->data;
	Str tier_name;
	json_read_string(&pos, &tier_name);
	tier_config_set_name(tier->config, &tier_name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
table_tier_rename(Table* self,
                  Tr*    tr,
                  Str*   name,
                  Str*   name_new,
                  bool   if_exists)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, name);
	if (! tier)
	{
		if (! if_exists)
			error("table '%.*s' tier '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// ensure new tier not exists
	if (tier_mgr_find(&self->tier_mgr, name_new))
		error("table '%.*s' tier '%.*s': already exists",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name_new),
		      str_of(name_new));

	// update table
	log_relation(&tr->log, &rename_if, tier, &self->rel);

	// save previous name
	encode_string(&tr->log.data, &tier->config->name);

	// rename tier
	tier_config_set_name(tier->config, name_new);
	return true;
}

static void
storage_add_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
storage_add_if_abort(Log* self, LogOp* op)
{
	Tier* tier = op->iface_arg;
	auto relation = log_relation_of(self, op);
	uint8_t* pos = relation->data;
	Str storage_name;
	json_read_string(&pos, &storage_name);

	auto volume = volume_mgr_find(&tier->config->volumes, &storage_name);
	assert(volume);
	error_catch (
		volume_rmdir(volume);
	);
	storage_unref(volume->storage);
	volume_mgr_remove(&tier->config->volumes, volume);
	volume_free(volume);
}

static LogIf storage_add_if =
{
	.commit = storage_add_if_commit,
	.abort  = storage_add_if_abort
};

bool
table_tier_storage_add(Table*  self,
                       Tr*     tr,
                       Str*    name,
                       Volume* config,
                       bool    if_exists,
                       bool    if_not_exists_storage)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, name);
	if (! tier)
	{
		if (! if_exists)
			error("table '%.*s' tier '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// find storage
	auto storage = storage_mgr_find(self->tier_mgr.storage_mgr, &config->name, true);

	// ensure storage not exists
	auto volume = volume_mgr_find(&tier->config->volumes, &config->name);
	if (volume)
	{
		if (! if_not_exists_storage)
			error("table '%.*s' tier '%.*s' storage '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name),
			      str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// update table
	log_relation(&tr->log, &storage_add_if, tier, &self->rel);

	// save storage name
	encode_string(&tr->log.data, &config->name);

	// add volume to the tier
	volume = volume_copy(config);
	volume->storage = storage;
	storage_ref(storage);
	volume_mgr_add(&tier->config->volumes, volume);

	// create directory
	volume_mkdir(volume);
	return true;
}

static void
storage_drop_if_commit(Log* self, LogOp* op)
{
	Tier* tier = op->iface_arg;
	auto relation = log_relation_of(self, op);
	uint8_t* pos = relation->data;
	Str storage_name;
	json_read_string(&pos, &storage_name);

	auto volume = volume_mgr_find(&tier->config->volumes, &storage_name);
	assert(volume);
	error_catch (
		volume_rmdir(volume);
	);
	storage_unref(volume->storage);
	volume_mgr_remove(&tier->config->volumes, volume);
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
table_tier_storage_drop(Table* self,
                        Tr*    tr,
                        Str*   name,
                        Str*   name_storage,
                        bool   if_exists,
                        bool   if_exists_storage)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, name);
	if (! tier)
	{
		if (! if_exists)
			error("table '%.*s' tier '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// ensure volume exists
	auto volume = volume_mgr_find(&tier->config->volumes, name_storage);
	if (! volume)
	{
		if (! if_exists_storage)
			error("table '%.*s' tier '%.*s' storage '%.*s': not found",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name),
			      str_size(name_storage),
			      str_of(name_storage));
		return false;
	}

	// ensure volume has no deps
	if (volume->refs > 0)
		error("table '%.*s' tier '%.*s' storage '%.*s': is not empty",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name),
		      str_size(name_storage),
		      str_of(name_storage));

	// update table
	log_relation(&tr->log, &storage_drop_if, tier, &self->rel);

	// save storage name
	encode_string(&tr->log.data, name_storage);
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
	Tier* tier = op->iface_arg;
	auto relation = log_relation_of(self, op);
	uint8_t* pos = relation->data;
	Str storage_name;
	json_read_string(&pos, &storage_name);

	auto volume = volume_mgr_find(&tier->config->volumes, &storage_name);
	assert(volume);
	volume_set_pause(volume, !volume->pause);
}

static LogIf storage_pause_if =
{
	.commit = storage_pause_if_commit,
	.abort  = storage_pause_if_abort
};

bool
table_tier_storage_pause(Table* self,
                         Tr*    tr,
                         Str*   name,
                         Str*   name_storage,
                         bool   pause,
                         bool   if_exists,
                         bool   if_exists_storage)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, name);
	if (! tier)
	{
		if (! if_exists)
			error("table '%.*s' tier '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// ensure storage exists
	auto volume = volume_mgr_find(&tier->config->volumes, name_storage);
	if (! volume)
	{
		if (! if_exists_storage)
			error("table '%.*s' tier '%.*s' storage '%.*s': not found",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name),
			      str_size(name_storage),
			      str_of(name_storage));
		return false;
	}

	if (volume->pause == pause)
		return false;

	// ensure at least one volume is still active
	if (pause && volume_mgr_count(&tier->config->volumes) <= 1)
		error("table '%.*s' tier '%.*s': at least one storage must remain active",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

	// update table
	log_relation(&tr->log, &storage_pause_if, tier, &self->rel);

	// apply
	volume_set_pause(volume, pause);

	// save storage name
	encode_string(&tr->log.data, name_storage);
	return true;
}

static void
set_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
set_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	// restore tier settings
	Tier* tier = op->iface_arg;
	uint8_t* pos = relation->data;

	auto config = tier_config_read(&pos);
	defer(tier_config_free, config);
	tier_config_set_region_size(tier->config, config->region_size);
	tier_config_set_object_size(tier->config, config->object_size);
}

static LogIf set_if =
{
	.commit = set_if_commit,
	.abort  = set_if_abort
};

bool
table_tier_set(Table*      self,
               Tr*         tr,
               TierConfig* config,
               int         mask,
               bool        if_exists)
{
	// find tier
	auto tier = tier_mgr_find(&self->tier_mgr, &config->name);
	if (! tier)
	{
		if (! if_exists)
			error("table '%.*s' tier '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// update table
	log_relation(&tr->log, &set_if, tier, &self->rel);

	// save current tier settings
	tier_config_write(tier->config, &tr->log.data, 0);

	// apply settings
	tier_config_set(tier->config, config, mask);
	return true;
}

Buf*
table_tier_list(Table* self, Str* ref, int flags)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show tier name on table
	if (ref)
	{
		auto tier = tier_mgr_find(&self->tier_mgr, ref);
		if (! tier)
			encode_null(buf);
		else
			tier_config_write(tier->config, buf, flags);
		return buf;
	}

	// show tiers on table
	encode_array(buf);
	list_foreach(&self->config->tiers)
	{
		auto config = list_at(TierConfig, link);
		tier_config_write(config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
