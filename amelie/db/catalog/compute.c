
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
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_grants(rel, &self->config->grants);
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
	check_user(tr, PERM_CREATE_COMPUTE);

	// make sure compute does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// allocate compute
	auto compute = compute_allocate(config);

	// update relations
	rels_create(&self->rels, tr, &compute->rel);
	return true;
}
