
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static inline void
layer_free(Layer* self, bool drop)
{
	auto table = self->table;
	auto timeline = self->config->timeline.timeline;

	// do table cleanup
	if (drop)
	{
		RpcSet set;
		rpc_set_init(&set);
		defer(rpc_set_free, &set);
		rpc_set_prepare(&set, table->parts.list_count);

		auto order = 0;
		list_foreach(&table->parts.list)
		{
			auto part = list_at(Part, link);
			auto msg  = part_cleanup_allocate(part, timeline);
			auto rpc  = rpc_set_add(&set, order, MSG_CLEANUP, msg);
			rpc_send(rpc, part->track.backend);
			order++;
		}
		rpc_set_wait(&set);
	}

	// detach and free vector stores
	auto columns = table_columns(table);
	if (columns->count_flat)
	{
		int64_t delta = 0;
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			if (column->type != TYPE_VECTOR)
				continue;
			list_foreach(&table->parts.list)
			{
				auto part = list_at(Part, link);
				auto flat = flats_remove(&part->flats, column, timeline);
				if (flat)
				{
					delta -= storage_size(&flat->storage);
					flat_free(flat);
				}
			}
		}
		usage_update(table->part_arg.memory, delta);
	}

	timelines_remove(&self->table->timelines, &self->config->timeline);
	layer_config_free(self->config);

	am_free(self);
}

static inline void
layer_show(Layer* self, Buf* buf, Str* user, int flags)
{
	if (flags_has(flags, FCREATE))
		describe(&self->rel, buf, user, flags);
	else
		layer_config_write(self->config, buf, flags);
}

static inline Layer*
layer_allocate(LayerConfig* config)
{
	auto self = (Layer*)am_malloc(sizeof(Layer));
	self->config = layer_config_copy(config);
	self->table  = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_LAYER);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)layer_show);
	rel_set_free(rel, (RelFree)layer_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static void
layer_open(Catalog* self, Layer* layer)
{
	// create vector stores
	auto table   = layer->table;
	auto columns = table_columns(table);
	if (! columns->count_flat)
		return;

	auto user     = layer->rel.user;
	auto name     = layer->rel.name;
	auto timeline = layer->config->timeline.timeline;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->type != TYPE_VECTOR)
			continue;
		list_foreach(&table->parts.list)
		{
			auto part = list_at(Part, link);
			auto flat = flat_allocate(column, timeline);
			flats_add(&part->flats, flat);

			// restore storage during recovery
			if (opt_int_of(&state()->recover) != RECOVER_CHECKPOINT)
				continue;

			// find partition manifest
			auto ref = manifest_find(self->manifest, user, name, column->order, part->config->id);
			if (! ref)
				error("recover: manifest is missing {str}.{str} partition {d}",
				      user, name, (int)part->config->id);

			auto storage = &flat->storage;
			storage_open(storage, state_checkpoint(), ref->pages_count, &ref->pages[0]);
			flat_open(flat);
			usage_update(table->part_arg.memory, storage_size(storage));

			auto total = (double)storage_size(storage) / 1024 / 1024;
			info("recover: {str}.{str} [{d}] ({d} column, {d} files, {.2f} MB)",
			     user, name, (int)part->config->id,
			     column->order,
			     storage->list_count,
			     total);
		}
	}
}

bool
layer_create(Catalog*     self,
             Tr*          tr,
             LayerConfig* config,
             bool         if_not_exists)
{
	// PERM_CREATE_TABLE
	catalog_check(self, tr, PERM_CREATE_TABLE, &config->user);

	// make sure layer does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// ensure table exists
	auto table = catalog_find_table(self, &config->table_user, &config->table, true);

	// ensure permission to create layer
	check_permission(tr, &table->rel, PERM_CREATE_TABLE);

	// validate grants
	catalog_grant_validate(self, tr, REL_LAYER,
	                       &config->user,
	                       &config->name, &config->grants);

	// check limit
	catalog_limit(self, tr, REL_LAYER, LIMIT_LAYERS);

	// create layer
	auto layer = layer_allocate(config);
	layer->table = table;
	layer->config->timeline.rel = &layer->rel;
	rels_create(&self->rels, tr, &layer->rel);

	// register layer timeline
	auto timelines = &table->timelines;
	timelines_add(timelines, &layer->config->timeline);

	// create vector stores
	layer_open(self, layer);
	return true;
}
