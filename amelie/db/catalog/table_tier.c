
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
table_tier_rmdir(Tier* tier)
{
	char path[PATH_MAX];
	char id[UUID_SZ];
	uuid_get(&tier->id, id, sizeof(id));
	list_foreach(&tier->storages)
	{
		// <storage_path>/<id_tier>
		auto storage = list_at(TierStorage, link);
		if (! storage->storage)
			continue;
		storage_pathfmt(storage->storage, path, "%s", id);
		if (fs_exists("%s", path))
			fs_rmdir("%s", path);
	}
}

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
	table_tier_rmdir(tier);
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
	char path[PATH_MAX];
	char id[UUID_SZ];
	uuid_get(&tier->id, id, sizeof(id));
	list_foreach(&tier->storages)
	{
		// <storage_path>/<id_tier>
		auto storage = list_at(TierStorage, link);
		storage_pathfmt(storage->storage, path, "%s", id);
		if (! fs_exists("%s", path))
			fs_mkdir(0755, "%s", path);
	}

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
	table_tier_rmdir(tier);
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

Buf*
table_tier_list(Table* self, Str* ref, bool extended)
{
	unused(extended);
	auto buf = buf_create();
	errdefer_buf(buf);

	// show tier name on table
	if (ref)
	{
		auto tier = table_tier_find(self, ref, false);
		if (! tier)
			encode_null(buf);
		else
			tier_write(tier, buf, false);
		return buf;
	}

	// show tiers on table
	encode_array(buf);
	list_foreach(&self->config->tiers)
	{
		auto tier = list_at(Tier, link);
		tier_write(tier, buf, false);
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
