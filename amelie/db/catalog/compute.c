
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static inline void
compute_free(Compute* self, bool drop)
{
	unused(drop);
	if (self->config)
		compute_config_free(self->config);
	am_free(self);
}

static inline void
compute_show(Compute* self, Buf* buf, int flags)
{
	compute_config_write(self->config, buf, flags);
}

static inline Compute*
compute_allocate(ComputeConfig* config)
{
	auto self = (Compute*)am_malloc(sizeof(Compute));
	self->config = compute_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_COMPUTE);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_show(rel, (RelShow)compute_show);
	rel_set_free(rel, (RelFree)compute_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
compute_create(Catalog*       self,
               Tr*            tr,
               ComputeConfig* config,
               bool           if_not_exists)
{
	// PERM_CREATE_COMPUTE
	//
	// (skip user check on bootstrap)
	if (tr->user)
		check_user(tr, PERM_CREATE_COMPUTE);

	// make sure compute does not exists
	auto compute = catalog_find_compute(self, &config->name, false);
	if (compute)
	{
		if (! if_not_exists)
			error("compute '{str}': already exists", &config->name);
		return false;
	}

	// allocate compute
	compute = compute_allocate(config);

	// update relations
	rels_create(&self->computes, tr, &compute->rel);
	return true;
}

bool
compute_drop(Catalog* self,
             Tr*      tr,
             Str*     name,
             bool     if_exists,
             bool     cascade)
{
	auto compute = catalog_find_compute(self, name, false);
	if (! compute)
	{
		if (! if_exists)
			error("compute '{str}': not exists", name);
		return false;
	}

	// system compute is immutable
	if (compute->config->system)
		error("compute '{str}': cannot be dropped", name);

	// only superuser
	check_user(tr, PERM_CREATE_COMPUTE);

	if (cascade)
	{
		// drop cascade all deps (tables and clones)
		Buf deps;
		buf_init(&deps);
		defer_buf(&deps);

		auto count = 0;
		list_foreach_safe(&self->rels.list)
		{
			auto rel = list_at(Rel, link);
			if (rel->type == REL_TABLE)
			{
				auto table = table_of(rel);
				if (! str_compare(&table->config->compute, name))
					continue;
			} else
			if (rel->type == REL_CLONE)
			{
				auto table = clone_of(rel)->table;
				if (! str_compare(&table->config->compute, name))
					continue;
			} else {
				continue;
			}

			count += catalog_deps(self, rel, &deps) + 1;
			if (! catalog_deps_has(&deps, rel))
				catalog_deps_add(&deps, rel);
		}

		// drop relations
		if (count > 0)
			catalog_deps_drop(self, tr, &deps);

	} else
	{
		// ensure no indirect dependecies on the compute name (table and clones)
		catalog_deps_validate_compute(self, &compute->config->name, true);
	}

	// drop compute by object
	rels_drop(&self->computes, tr, &compute->rel);
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
	Str name_old;
	unpack_str(&pos, &name_old);

	Catalog* catalog = op->iface_arg;

	// rename references
	list_foreach_safe(&catalog->rels.list)
	{
		auto rel = list_at(Rel, link);

		TableConfig* config = NULL;
		if (rel->type == REL_TABLE)
		{
			auto table = table_of(rel);
			if (str_compare(&table->config->compute, op->rel->name))
				config = table->config;
		} else
		if (rel->type == REL_CLONE)
		{
			auto table = clone_of(rel)->table;
			if (str_compare(&table->config->compute, op->rel->name))
				config = table->config;
		}
		if (! config)
			continue;

		table_config_set_compute(config, &name_old);
	}

	rels_rename(&catalog->computes, op->rel, NULL, &name_old);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
compute_rename(Catalog* self,
               Tr*      tr,
               Str*     name,
               Str*     name_new,
               bool     if_exists)
{
	auto compute = catalog_find_compute(self, name, false);
	if (! compute)
	{
		if (! if_exists)
			error("compute '{str}': not exists", name);
		return false;
	}

	// system compute is immutable
	if (compute->config->system)
		error("compute '{str}': cannot be renamed", name);

	// only superuser
	check_user(tr, PERM_CREATE_COMPUTE);

	// ensure new compute does not exists
	if (catalog_find_compute(self, name_new, false))
		error("compute '{str}': already exists", name_new);

	// update compute
	log_ddl(&tr->log, &rename_if, self, &compute->rel);

	// save name for rollback
	encode_str(&tr->log.data, name);

	// set new name
	rels_rename(&self->computes, &compute->rel, NULL, name_new);

	// rename references
	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);

		TableConfig* config = NULL;
		if (rel->type == REL_TABLE)
		{
			auto table = table_of(rel);
			if (str_compare(&table->config->compute, name))
				config = table->config;
		} else
		if (rel->type == REL_CLONE)
		{
			auto table = clone_of(rel)->table;
			if (str_compare(&table->config->compute, name))
				config = table->config;
		}
		if (! config)
			continue;

		table_config_set_compute(config, name_new);
	}

	return true;
}
