
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
sidetable_free(Sidetable* self, bool drop)
{
	auto id = self->config->timeline.timeline;
	timelines_remove(&self->table->timelines, &self->config->timeline);
	sidetable_config_free(self->config);

	// do table cleanup
	if (drop)
	{
		auto table = self->table;

		RpcSet set;
		rpc_set_init(&set);
		defer(rpc_set_free, &set);
		rpc_set_prepare(&set, table->parts.list_count);

		auto order = 0;
		list_foreach(&table->parts.list)
		{
			auto part = list_at(Part, link);
			auto msg  = part_cleanup_allocate(part, id);
			auto rpc  = rpc_set_add(&set, order, MSG_CLEANUP, msg);
			rpc_send(rpc, part->track.backend);
			order++;
		}
		rpc_set_wait(&set);
	}

	am_free(self);
}

static inline void
sidetable_show(Sidetable* self, Buf* buf, Str* user, int flags)
{
	if (flags_has(flags, FCREATE))
		describe(&self->rel, buf, user, flags);
	else
		sidetable_config_write(self->config, buf, flags);
}

static inline Sidetable*
sidetable_allocate(SidetableConfig* config)
{
	auto self = (Sidetable*)am_malloc(sizeof(Sidetable));
	self->config = sidetable_config_copy(config);
	self->table  = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_SIDETABLE);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)sidetable_show);
	rel_set_free(rel, (RelFree)sidetable_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
sidetable_create(Catalog*         self,
                 Tr*              tr,
                 SidetableConfig* config,
                 bool             if_not_exists)
{
	// PERM_CREATE_TABLE
	catalog_check(self, tr, PERM_CREATE_TABLE, &config->user);

	// make sure sidetable does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// ensure table exists
	auto table = catalog_find_table(self, &config->table_user, &config->table, true);

	// ensure permission to create sidetable
	check_permission(tr, &table->rel, PERM_CREATE_TABLE);

	// validate grants
	catalog_grant_validate(self, tr, REL_SIDETABLE,
	                       &config->user,
	                       &config->name, &config->grants);

	// check limit
	catalog_limit(self, tr, REL_SIDETABLE, LIMIT_SIDETABLES);

	// ensure table has no vector columns
	list_foreach(&table_columns(table)->list)
	{
		auto column = list_at(Column, link);
		if (column->type == TYPE_VECTOR)
			error("table '{str}': vector columns cannot be used together with sidetables",
			      &config->name);
	}

	// create sidetable
	auto sidetable = sidetable_allocate(config);
	sidetable->table = table;
	sidetable->config->timeline.rel = &sidetable->rel;
	rels_create(&self->rels, tr, &sidetable->rel);

	// register sidetable timeline
	auto timelines = &table->timelines;
	timelines_add(timelines, &sidetable->config->timeline);
	return true;
}
