
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>

void
cascade_table_drop(Db* self, Transaction* trx, Str* schema, Str* name,
                   bool if_exists)
{
	table_mgr_drop(&self->table_mgr, trx, schema, name, if_exists);
}

static void
cascade_drop(Db* self, Transaction* trx, Str* schema)
{
	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (str_compare(&table->config->schema, schema))
			table_mgr_drop_of(&self->table_mgr, trx, table);
	}

	// views
	list_foreach_safe(&self->view_mgr.mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		if (str_compare(&view->config->schema, schema))
			view_mgr_drop(&self->view_mgr, trx, &view->config->schema,
			              &view->config->name, false);
	}
}

static void
cascade_rename(Db* self, Transaction* trx, Str* schema, Str* schema_new)
{
	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (str_compare(&table->config->schema, schema))
			table_mgr_rename(&self->table_mgr, trx, &table->config->schema,
			                 &table->config->name,
			                 schema_new,
			                 &table->config->name, false);
	}

	// views
	list_foreach_safe(&self->view_mgr.mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		if (str_compare(&view->config->schema, schema))
			view_mgr_rename(&self->view_mgr, trx, &view->config->schema,
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
cascade_schema_drop(Db*          self,
                    Transaction* trx,
                    Str*         name,
                    bool         cascade,
                    bool         if_exists)
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
		cascade_drop(self, trx, name);
	} else
	{
		// ensure no dependencies
		cascade_schema_validate(self, name);
	}

	schema_mgr_drop(&self->schema_mgr, trx, name, false);
}

void
cascade_schema_rename(Db*           self,
                      Transaction*  trx,
                      Str*          name,
                      Str*          name_new,
                      bool          if_exists)
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
	cascade_rename(self, trx, name, name_new);

	// rename schema last
	schema_mgr_rename(&self->schema_mgr, trx, name, name_new, false);
}
