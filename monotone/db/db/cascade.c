
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

static void
cascade_drop(Db* self, Transaction* trx, Str* schema)
{
	(void)self;
	(void)trx;
	(void)schema;
	// todo:
}

static void
cascade_alter(Db* self, Transaction* trx, Str* schema, Str* schema_new)
{
	(void)self;
	(void)trx;
	(void)schema;
	(void)schema_new;
	// todo:
}

static void
cascade_schema_validate(Db* self, Str* schema)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (str_compare(&table->config->schema, schema))
			error("table '%.*s': has schema '%.*s", str_size(&table->config->name),
			      str_of(&table->config->name),
			      str_size(schema), str_of(schema));
	}
	list_foreach(&self->meta_mgr.mgr.list)
	{
		auto meta = meta_of(list_at(Handle, link));
		if (str_compare(&meta->config->schema, schema))
			error("view '%.*s': has schema '%.*s", str_size(&meta->config->name),
			      str_of(&meta->config->name),
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
cascade_schema_alter(Db*           self,
                     Transaction*  trx,
                     Str*          name,
                     SchemaConfig* config,
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
	cascade_alter(self, trx, name, &config->name);

	schema_mgr_alter(&self->schema_mgr, trx, name, config, false);
}
