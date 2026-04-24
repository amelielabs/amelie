
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
sub_free(Sub* self, bool drop)
{
	unused(drop);
	catalog_cdc_unref(self->catalog, &self->on_id);
	cdc_detach(self->cdc, &self->slot);
	sub_config_free(self->config);
	am_free(self);
}

Sub*
sub_allocate(SubConfig* config, Catalog* catalog, Cdc* cdc, Uuid* id)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->config  = sub_config_copy(config);
	self->cdc     = cdc;
	self->catalog = catalog;
	self->on_id   = *id;
	cdc_slot_init(&self->slot);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_SUBSCRIPTION);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_free_function(rel, (RelFree)sub_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}
