
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
sub_unref(Sub* self)
{
	auto table = table_mgr_find_by(&self->catalog->table_mgr, &self->on_id, false);
	if (table)
	{
		table->part_arg.cdc--;
		assert(table->part_arg.cdc >= 0);
		return;
	}
	auto topic = topic_mgr_find_by(&self->catalog->topic_mgr, &self->on_id, false);
	if (topic)
	{
		topic->cdc--;
		assert(topic->cdc >= 0);
		return;
	}
}

static void
sub_free(Sub* self, bool drop)
{
	unused(drop);
	sub_unref(self);
	cdc_detach(self->cdc, &self->slot);
	sub_config_free(self->config);
	am_free(self);
}

Sub*
sub_allocate(SubConfig* config, Catalog* catalog, Cdc* cdc)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->config  = sub_config_copy(config);
	self->cdc     = cdc;
	self->catalog = catalog;
	uuid_init(&self->on_id);
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
