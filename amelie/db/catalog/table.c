
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
table_free(Table* self, bool drop)
{
	unused(drop);
	if (self->part)
	{
		part_free(self->part);
		self->part = NULL;
	}
	if (self->config)
		table_config_free(self->config);
	am_free(self);
}

static inline void
table_show(Table* self, Buf* buf, Str* user, int flags)
{
	if (flags_has(flags, FCREATE))
		describe(&self->rel, buf, user, flags);
	else
		table_config_write(self->config, buf, flags);
}

static inline Table*
table_allocate(Catalog*     catalog,
               User*        user,
               TableConfig* config,
               Usage*       memory)
{
	auto self = (Table*)am_malloc(sizeof(Table));
	self->config  = table_config_copy(config);
	self->catalog = catalog;
	self->part    = NULL;
	self->user    = user;

	// part context
	auto arg = &self->part_arg;
	arg->rel       = &self->rel;
	arg->memory    = memory;
	arg->columns   = &self->config->columns;
	arg->retention = &self->retention;
	arg->timelines = &self->timelines;

	// retention
	retention_init(&self->retention);
	retention_set(&self->retention, self->config->retention);

	// timelines
	timelines_init(&self->timelines, &self->rel);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_TABLE);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)table_show);
	rel_set_free(rel, (RelFree)table_free);
	return self;
}

static void
table_open(Table* self)
{
	// create partition
	auto part = part_allocate(&self->part_arg);
	self->part = part;

	// create indexes and stores
	list_foreach(&self->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		part_index_create(part, config);
	}

	// create flat stores
	list_foreach(&self->config->columns.list)
	{
		auto column = list_at(Column, link);
		if (! column->size_flat)
			continue;
		auto flat = flat_allocate(column, 0);
		flats_add_column(&part->flats);
		flats_add(&part->flats, flat);
	}

	// open partition files
	if (opt_int_of(&state()->recover) == RECOVER_CHECKPOINT)
		part_open(part, self->catalog->manifest);
}

bool
table_create(Catalog*     self,
             Tr*          tr,
             TableConfig* config,
             bool         if_not_exists)
{
	// PERM_CREATE_TABLE
	catalog_check(self, tr, PERM_CREATE_TABLE, &config->user);

	// make sure table does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// check limit
	catalog_limit(self, tr, REL_TABLE, LIMIT_TABLES);

	// validate grants
	catalog_grant_validate(self, tr, REL_TABLE,
	                       &config->user,
	                       &config->name, &config->grants);

	// validate retention
	if (config->retention >= 0 && config->indexes_count > 0)
		error("table '{str}': using indexes with retention is not supported",
		      &config->name);

	// allocate table
	auto user  = user_of(tr->user);
	auto table = table_allocate(self, user, config, &user->memory);

	// update tables
	rels_create(&self->rels, tr, &table->rel);

	// prepare table and recover
	table_open(table);
	return true;
}

static void
truncate_if_commit(Log* self, LogOp* op)
{
	unused(self);
	auto table = table_of(op->rel);

	table->catalog->iface->table_truncate(table->catalog, table);
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
	self->iface->user_sync(self, table->user);

	// do nothing (actual truncate will happen on commit)
	return true;
}
