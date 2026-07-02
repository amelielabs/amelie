
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
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static inline void
table_free(Table* self, bool drop)
{
	unused(drop);
	auto parts = &self->parts;
	parts_close(parts);
	parts_free(parts);

	sequence_free(&self->seq);
	if (self->config)
		table_config_free(self->config);
	am_free(self);
}

static inline void
table_show(Table* self, Buf* buf, int flags)
{
	table_config_write(self->config, buf, flags);
}

static inline Table*
table_allocate(TableConfig* config,
               PartsIf*     iface,
               void*        iface_arg)
{
	auto self = (Table*)am_malloc(sizeof(Table));
	self->config = table_config_copy(config);
	sequence_init(&self->seq);

	// part context
	auto arg = &self->part_arg;
	arg->seq       = &self->seq;
	arg->rel       = &self->rel;
	arg->snapshots = &self->snapshots;

	// partition manager
	auto primary = table_primary(self);
	parts_init(&self->parts, iface, iface_arg, arg, &primary->keys);

	// snapshot manager
	snapshots_init(&self->snapshots, &self->rel);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_TABLE);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_id(rel, &self->config->id);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)table_show);
	rel_set_free(rel, (RelFree)table_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
table_create(Catalog*     self,
             Tr*          tr,
             TableConfig* config,
             bool         if_not_exists)
{
	// PERM_CREATE_TABLE
	check_user(tr, PERM_CREATE_TABLE);

	// make sure table does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// allocate table
	auto table = table_allocate(config, self->iface_part, self->iface_part_arg);

	// update tables
	rels_create(&self->rels, tr, &table->rel);

	// recover, map and deploy partitions
	parts_open(&table->parts, &table->config->parts,
	           &table->config->indexes);
	return true;
}

static void
truncate_if_commit(Log* self, LogOp* op)
{
	unused(self);
	// truncate partitions
	auto table = table_of(op->rel);
	parts_truncate(&table->parts);
}

static void
truncate_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf truncate_if =
{
	.commit = truncate_if_commit,
	.abort  = truncate_if_abort
};

bool
table_truncate(Catalog* self,
               Tr*      tr,
               Str*     user,
               Str*     name,
               bool     if_exists)
{
	auto table = catalog_find_table(self, user, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &table->rel);

	// update table
	log_ddl(&tr->log, &truncate_if, NULL, &table->rel);

	// force commit pending prepared transactions
	list_foreach(&table->parts.list)
	{
		auto part = list_at(Part, link);
		auto consensus = &part->track.consensus;
		track_sync(&part->track, consensus);
	}

	// do nothing (actual truncate will happen on commit)
	return true;
}
