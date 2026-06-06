
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
sub_free(Sub* self, bool drop)
{
	unused(drop);
	auto catalog = self->catalog;
	if (self->rel_on)
	{
		self->rel_on->subs--;
		assert(self->rel_on->subs >= 0);
	}
	cdc_detach(catalog->cdc, &self->slot);
	sub_config_free(self->config);
	am_free(self);
}

static inline void
sub_show(Sub* self, Buf* buf, int flags)
{
	sub_config_write(self->config, buf, flags);
}

static Sub*
sub_allocate(SubConfig* config, Catalog* catalog, Rel* rel_on)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->rel_on  = rel_on;
	self->config  = sub_config_copy(config);
	self->catalog = catalog;
	cdc_slot_init(&self->slot);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_SUBSCRIPTION);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_id(rel, &self->config->id);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)sub_show);
	rel_set_free(rel, (RelFree)sub_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
sub_create(Catalog* self, Tr* tr, SubConfig* config, bool if_not_exists)
{
	// PERM_CREATE_SUBSCRIPTION
	check_user(tr, PERM_CREATE_SUBSCRIPTION);

	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// find and validate relation
	auto on = catalog_find(self, REL_UNDEF, &config->rel_user, &config->rel, true);
	auto unlogged = false;
	switch (on->type) {
	case REL_TABLE:
	{
		auto table = table_of(on);
		unlogged = table->config->unlogged;
		break;
	}
	case REL_CLONE:
	{
		auto clone = clone_of(on);
		unlogged = clone->table->config->unlogged;
		break;
	}
	case REL_TOPIC:
	{
		auto topic = topic_of(on);
		unlogged = topic->config->unlogged;
		break;
	}
	default:
		error("subscription '{str}': cannot be used with {s}", &config->name,
		      rel_type_of(on->type));
		break;
	}
	if (unlogged)
		error("subscription '{str}': unlogged {s} cannot be used with subscription",
		      &config->name, rel_type_of(on->type));

	// check permission
	check_permission(tr, on, PERM_CREATE_SUBSCRIPTION);

	// create subscription
	auto sub = sub_allocate(config, self, on);
	rel_mgr_create(&self->rels, tr, &sub->rel);
	on->subs++;

	// set pos and prepare slot
	cdc_slot_set(&sub->slot, config->lsn, config->lsn_op);

	// attach slot
	cdc_attach(self->cdc, &sub->slot);
	return true;
}
