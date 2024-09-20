
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
cascade_table_drop(Db* self, Tr* tr, Str* schema, Str* name,
                   bool if_exists)
{
	table_mgr_drop(&self->table_mgr, tr, schema, name, if_exists);
}

static void
cascade_drop(Db* self, Tr* tr, Str* schema)
{
	// udfs
	list_foreach_safe(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Handle, link));
		if (str_compare(&udf->config->schema, schema))
			udf_mgr_drop(&self->udf_mgr, tr, &udf->config->schema,
			             &udf->config->name, false);
	}

	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (str_compare(&table->config->schema, schema))
			table_mgr_drop_of(&self->table_mgr, tr, table);
	}

	// views
	list_foreach_safe(&self->view_mgr.mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		if (str_compare(&view->config->schema, schema))
			view_mgr_drop(&self->view_mgr, tr, &view->config->schema,
			              &view->config->name, false);
	}
}

static void
cascade_rename(Db* self, Tr* tr, Str* schema, Str* schema_new)
{
	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (str_compare(&table->config->schema, schema))
			table_mgr_rename(&self->table_mgr, tr, &table->config->schema,
			                 &table->config->name,
			                 schema_new,
			                 &table->config->name, false);
	}

	// views
	list_foreach_safe(&self->view_mgr.mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		if (str_compare(&view->config->schema, schema))
			view_mgr_rename(&self->view_mgr, tr, &view->config->schema,
			                &view->config->name,
			                schema_new,
			                &view->config->name, false);
	}
}

static void
cascade_schema_validate(Db* self, Str* schema)
{
	// tables
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (str_compare(&table->config->schema, schema))
			error("table '%.*s' depends on schema '%.*s", str_size(&table->config->name),
			      str_of(&table->config->name),
			      str_size(schema), str_of(schema));
	}

	// views
	list_foreach(&self->view_mgr.mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		if (str_compare(&view->config->schema, schema))
			error("view '%.*s' depends on schema '%.*s", str_size(&view->config->name),
			      str_of(&view->config->name),
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
