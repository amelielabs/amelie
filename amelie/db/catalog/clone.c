
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
clone_free(Clone* self, bool drop)
{
	unused(drop);
	snapshot_mgr_remove(&self->table->snapshot_mgr, &self->config->snapshot);
	clone_config_free(self->config);
	am_free(self);
}

static inline void
clone_show(Clone* self, Buf* buf, int flags)
{
	clone_config_write(self->config, buf, flags);
}

static inline Clone*
clone_allocate(CloneConfig* config)
{
	auto self = (Clone*)am_malloc(sizeof(Clone));
	self->config = clone_config_copy(config);
	self->table  = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_CLONE);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_id(rel, &self->config->id);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)clone_show);
	rel_set_free(rel, (RelFree)clone_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
clone_create(Catalog*     self,
             Tr*          tr,
             CloneConfig* config,
             bool         if_not_exists)
{
	// PERM_CREATE_CLONE
	check_user(tr, PERM_CREATE_CLONE);

	// make sure clone does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// ensure table exists
	auto table = catalog_find_table(self, &config->table_user, &config->table, true);

	// ensure permission to create clone
	check_permission(tr, &table->rel, PERM_CREATE_CLONE);

	// create clone
	auto clone = clone_allocate(config);
	clone->table = table;
	clone->config->snapshot.main = &table->snapshot_mgr.main;
	clone->config->snapshot.rel  = &clone->rel;
	rel_mgr_create(&self->rels, tr, &clone->rel);

	// register clone snapshot
	snapshot_mgr_add(&table->snapshot_mgr, &clone->config->snapshot);
	return true;
}
