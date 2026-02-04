
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>
#include <amelie_catalog.h>

static void
table_tier_delete(Table* table, Tier* tier)
{
	// unref storages
	tier_unref(tier);

	// remove tier from engine
	engine_tier_remove(&table->engine, &tier->name);

	// remove tier from the table config
	table_config_tier_remove(table->config, tier);

	// free
	tier_free(tier);
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
	error_catch (
		tier_rmdir(tier)
	);
	table_tier_delete(table, tier);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

bool
table_tier_create(Table* self,
                  Tr*    tr,
                  Tier*  config,
                  bool   if_not_exists)
{
	// find tier
	auto tier = table_tier_find(self, &config->name, false);
	if (tier)
	{
		if (! if_not_exists)
			error("table '%.*s' tier '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(&tier->name),
			      str_of(&tier->name));
		return false;
	}

	// save tier copy to the table config
	tier = tier_copy(config);
	table_config_tier_add(self->config, tier);

	// update table
	log_relation(&tr->log, &create_if, tier, &self->rel);

	// resolve tier storages
	tier_ref(tier, self->engine.storage_mgr);

	// create directories
	tier_mkdir(tier);

	// update engine
	engine_tier_add(&self->engine, tier);
	return true;
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	Tier* tier = op->iface_arg;
	error_catch (
		tier_rmdir(tier)
	);
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
	auto tier = table_tier_find(self, name, false);
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

	// validate engine tiers
	auto level = engine_tier_find(&self->engine, name);
	assert(level);
	if (level->list_count > 0)
		error("table '%.*s' tier '%.*s': is not empty",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

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
	tier_set_name(tier, &tier_name);
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
	auto tier = table_tier_find(self, name, false);
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
	if (table_tier_find(self, name_new, false))
		error("table '%.*s' tier '%.*s': already exists",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name_new),
		      str_of(name_new));

	// update table
	log_relation(&tr->log, &rename_if, tier, &self->rel);

	// save previous name
	encode_string(&tr->log.data, &tier->name);

	// rename tier
	tier_set_name(tier, name_new);
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

	auto tier_storage = tier_storage_find(tier, &storage_name);
	assert(tier_storage);
	storage_unref(tier_storage->storage);
	error_catch (
		tier_storage_rmdir(tier, tier_storage);
	);
	tier_storage_remove(tier, tier_storage);
	tier_storage_free(tier_storage);
}

static LogIf storage_add_if =
{
	.commit = storage_add_if_commit,
	.abort  = storage_add_if_abort
};

bool
table_tier_storage_add(Table* self,
                       Tr*    tr,
                       Str*   name,
                       Str*   name_storage,
                       bool   if_exists,
                       bool   if_not_exists_storage)
{
	// find tier
	auto tier = table_tier_find(self, name, false);
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
	auto storage = storage_mgr_find(self->engine.storage_mgr, name_storage, true);

	// ensure storage not exists
	auto tier_storage = tier_storage_find(tier, name_storage);
	if (tier_storage)
	{
		if (! if_not_exists_storage)
			error("table '%.*s' tier '%.*s' storage '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name),
			      str_size(name_storage),
			      str_of(name_storage));
		return false;
	}

	// update table
	log_relation(&tr->log, &storage_add_if, tier, &self->rel);

	// save storage name
	encode_string(&tr->log.data, name_storage);

	// add storage to the tier
	tier_storage = tier_storage_allocate();
	tier_storage_set_name(tier_storage, name_storage);
	tier_storage->storage = storage;
	tier_storage_add(tier, tier_storage);
	storage_ref(storage);

	// create directory
	tier_storage_mkdir(tier, tier_storage);
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

	auto tier_storage = tier_storage_find(tier, &storage_name);
	assert(tier_storage);
	storage_unref(tier_storage->storage);
	error_catch (
		tier_storage_rmdir(tier, tier_storage);
	);
	tier_storage_remove(tier, tier_storage);
	tier_storage_free(tier_storage);
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
	auto tier = table_tier_find(self, name, false);
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
	auto tier_storage = tier_storage_find(tier, name_storage);
	if (! tier_storage)
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

	// ensure tier storage has no deps
	if (tier_storage->refs > 0)
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

	auto tier_storage = tier_storage_find(tier, &storage_name);
	tier_storage->pause = !tier_storage->pause;
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
	auto tier = table_tier_find(self, name, false);
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
	auto tier_storage = tier_storage_find(tier, name_storage);
	if (! tier_storage)
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

	if (tier_storage->pause == pause)
		return false;

	// ensure at least one tier storage is still active
	if (pause && tier_storage_count(tier) <= 1)
		error("table '%.*s' tier '%.*s': at least one storage must remain active",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

	// update table
	log_relation(&tr->log, &storage_pause_if, tier, &self->rel);

	// apply
	tier_storage->pause = pause;

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
	tier_free_data(tier);
	uint8_t* pos = relation->data;
	auto tier_copy = tier_read(&pos);
	defer(tier_free, tier_copy);
	tier_copy_to(tier_copy, tier);
}

static LogIf set_if =
{
	.commit = set_if_commit,
	.abort  = set_if_abort
};

bool
table_tier_set(Table* self,
               Tr*    tr,
               Tier*  config,
               int    mask,
               bool   if_exists)
{
	// find tier
	auto tier = table_tier_find(self, &config->name, false);
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
	tier_write(tier, &tr->log.data, 0);

	// apply settings
	tier_set(tier, config, mask);
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
		auto tier = table_tier_find(self, ref, false);
		if (! tier)
			encode_null(buf);
		else
			tier_write(tier, buf, flags);
		return buf;
	}

	// show tiers on table
	encode_array(buf);
	list_foreach(&self->config->tiers)
	{
		auto tier = list_at(Tier, link);
		tier_write(tier, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}

Tier*
table_tier_find(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->tiers)
	{
		auto tier = list_at(Tier, link);
		if (str_compare_case(&tier->name, name))
			return tier;
	}
	if (error_if_not_exists)
		error("tier '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}
