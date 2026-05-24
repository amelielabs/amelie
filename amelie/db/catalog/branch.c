
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

static inline void
branch_free(Branch* self, bool drop)
{
	unused(drop);
	snapshot_mgr_remove(&self->table->snapshot_mgr, &self->config->snapshot);
	branch_config_free(self->config);
	am_free(self);
}

static inline void
branch_show(Branch* self, Buf* buf, int flags)
{
	branch_config_write(self->config, buf, flags);
}

static inline Branch*
branch_allocate(BranchConfig* config)
{
	auto self = (Branch*)am_malloc(sizeof(Branch));
	self->config = branch_config_copy(config);
	self->table  = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_BRANCH);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)branch_show);
	rel_set_free(rel, (RelFree)branch_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
branch_create(Catalog*      self,
              Tr*           tr,
              BranchConfig* config,
              bool          if_not_exists)
{
	// PERM_CREATE_BRANCH
	check_user(tr, PERM_CREATE_BRANCH);

	// make sure branch does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// ensure table exists
	auto table = catalog_find_table(self, &config->table_user, &config->table, true);

	// ensure permission to create branch on the table
	check_permission(tr, &table->rel, PERM_CREATE_BRANCH);

	// create branch
	auto branch = branch_allocate(config);
	branch->table = table;
	branch->config->snapshot.main = &table->snapshot_mgr.main;
	branch->config->snapshot.rel  = &branch->rel;
	rel_mgr_create(&self->rels, tr, &branch->rel);

	// register branch snapshot
	snapshot_mgr_add(&table->snapshot_mgr, &branch->config->snapshot);
	return true;
}
