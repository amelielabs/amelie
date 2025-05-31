
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

static void
cascade_drop(Db* self, Tr* tr, Str* schema)
{
	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		if (str_compare(&table->config->schema, schema))
			table_mgr_drop_of(&self->table_mgr, tr, table);
	}

	// procs
	list_foreach_safe(&self->proc_mgr.mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		if (str_compare(&proc->config->schema, schema))
			proc_mgr_drop_of(&self->proc_mgr, tr, proc);
	}
}

static void
cascade_rename(Db* self, Tr* tr, Str* schema, Str* schema_new)
{
	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		if (str_compare(&table->config->schema, schema))
			table_mgr_rename(&self->table_mgr, tr, &table->config->schema,
			                 &table->config->name,
			                 schema_new,
			                 &table->config->name, false);
	}

	// procs
	list_foreach_safe(&self->proc_mgr.mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		if (str_compare(&proc->config->schema, schema))
			proc_mgr_rename(&self->proc_mgr, tr, &proc->config->schema,
			                &proc->config->name,
			                schema_new,
			                &proc->config->name, false);
	}
}

static void
cascade_schema_validate_external(Db* self, Str* schema)
{
	// ensure that no external schema procs depend on the procs
	// of the dropped schema
	list_foreach(&self->proc_mgr.mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		if (str_compare(&proc->config->schema, schema))
			continue;
		if (proc_depend(proc, schema, NULL))
			error("procedure '%.*s.%.*s' depends on schema '%.*s", str_size(&proc->config->schema),
			      str_of(&proc->config->schema),
			      str_size(&proc->config->name),
			      str_of(&proc->config->name),
			      str_size(schema), str_of(schema));
	}
}

static void
cascade_schema_validate(Db* self, Str* schema)
{
	// tables
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		if (str_compare(&table->config->schema, schema))
			error("table '%.*s' depends on schema '%.*s", str_size(&table->config->name),
			      str_of(&table->config->name),
			      str_size(schema), str_of(schema));
	}

	// procs
	list_foreach(&self->proc_mgr.mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		if (str_compare(&proc->config->schema, schema))
			error("procedure '%.*s' depends on schema '%.*s", str_size(&proc->config->name),
			      str_of(&proc->config->name),
			      str_size(schema), str_of(schema));
	}
}

void
cascade_schema_drop(Db*  self, Tr* tr, Str* name,
                    bool cascade,
                    bool if_exists)
{
	auto schema = schema_mgr_find(&self->schema_mgr, name, false);
	if (! schema)
	{
		if (! if_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	if (schema->config->system)
		error("schema '%.*s': system schema cannot be dropped", str_size(name),
		       str_of(name));

	if (cascade)
	{
		// ensure no external dependencies
		cascade_schema_validate_external(self, name);

		// drop all dependencies
		cascade_drop(self, tr, name);
	} else
	{
		// ensure no dependencies
		cascade_schema_validate(self, name);
	}

	schema_mgr_drop(&self->schema_mgr, tr, name, false);
}

void
cascade_schema_rename(Db*  self, Tr* tr, Str* name,
                      Str* name_new,
                      bool if_exists)
{
	auto schema = schema_mgr_find(&self->schema_mgr, name, false);
	if (! schema)
	{
		if (! if_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	if (schema->config->system)
		error("schema '%.*s': system schema cannot be altered", str_size(name),
		      str_of(name));

	// rename schema on all dependable objects
	cascade_rename(self, tr, name, name_new);

	// rename schema last
	schema_mgr_rename(&self->schema_mgr, tr, name, name_new, false);
}
