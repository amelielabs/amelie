
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_partition.h>
#include <amelie_tier.h>

void
tier_mgr_init(TierMgr* self)
{
	relation_mgr_init(&self->mgr);
}

void
tier_mgr_free(TierMgr* self)
{
	relation_mgr_free(&self->mgr);
}

bool
tier_mgr_create(TierMgr* self,
                Tr*      tr,
                Source*  config,
                bool     if_not_exists)
{
	// make sure tier does not exists
	auto current = tier_mgr_find(self, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("tier '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create tier directory, if not exists
	char path[PATH_MAX];
	source_basepath(config, path);
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// allocate tier and init
	auto tier = tier_allocate(config);

	// register tier
	relation_mgr_create(&self->mgr, tr, &tier->rel);
	return true;
}

bool
tier_mgr_drop(TierMgr* self,
              Tr*      tr,
              Str*     name,
              bool     if_exists)
{
	auto tier = tier_mgr_find(self, name, false);
	if (! tier)
	{
		if (! if_exists)
			error("tier '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	if (tier->refs > 0)
		error("tier '%.*s': is being used", str_size(name),
		      str_of(name));

	if (tier->config->system)
		error("tier '%.*s': system tier cannot be dropped", str_size(name),
		      str_of(name));

	// drop tier by object
	relation_mgr_drop(&self->mgr, tr, &tier->rel);
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
	auto mgr = tier_of(relation->relation);
	// set previous name
	uint8_t* pos = relation->data;
	Str name;
	json_read_string(&pos, &name);
	source_set_name(mgr->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
tier_mgr_rename(TierMgr* self,
                Tr*      tr,
                Str*     name,
                Str*     name_new,
                bool     if_exists)
{
	auto tier = tier_mgr_find(self, name, false);
	if (! tier)
	{
		if (! if_exists)
			error("tier '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (tier->refs > 0)
		error("tier '%.*s': is being used", str_size(name),
		      str_of(name));

	if (tier->config->system)
		error("tier '%.*s': system tier cannot be renamed", str_size(name),
		       str_of(name));

	// ensure new tier does not exists
	if (tier_mgr_find(self, name_new, false))
		error("tier '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update tier
	log_relation(&tr->log, &rename_if, NULL, &tier->rel);

	// save name for rollback
	encode_string(&tr->log.data, name);

	// set new name
	source_set_name(tier->config, name_new);
	return true;
}

void
tier_mgr_dump(TierMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto tier = tier_of(list_at(Relation, link));
		if (tier->config->system)
			continue;
		source_write(tier->config, buf, false);
	}
	encode_array_end(buf);
}

Tier*
tier_mgr_find(TierMgr* self, Str* name, bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, NULL, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("tier '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return tier_of(relation);
}

Buf*
tier_mgr_list(TierMgr* self, Str* name, bool extended)
{
	unused(extended);
	auto buf = buf_create();
	if (name)
	{
		auto tier = tier_mgr_find(self, name, false);
		if (tier)
			source_write(tier->config, buf, true);
		else
			encode_null(buf);
	} else
	{
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto tier = tier_of(list_at(Relation, link));
			source_write(tier->config, buf, true);
		}
		encode_array_end(buf);
	}
	return buf;
}
