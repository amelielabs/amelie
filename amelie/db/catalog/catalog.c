
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_env.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>

void
catalog_init(Catalog*   self, PartMgr* part_mgr,
             CatalogIf* iface,
             void*      iface_arg)
{
	self->iface = iface;
	self->iface_arg = iface_arg;
	schema_mgr_init(&self->schema_mgr);
	table_mgr_init(&self->table_mgr, part_mgr);
}

void
catalog_free(Catalog* self)
{
	table_mgr_free(&self->table_mgr);
	schema_mgr_free(&self->schema_mgr);
}

static void
catalog_create_system_schema(Catalog* self, const char* schema, bool create)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		// create system schema
		Str name;
		str_init(&name);
		str_set_cstr(&name, schema);
		auto config = schema_config_allocate();
		defer(schema_config_free, config);
		schema_config_set_name(config, &name);
		schema_config_set_system(config, true);
		schema_config_set_create(config, create);
		schema_mgr_create(&self->schema_mgr, &tr, config, false);

		// commit
		tr_commit(&tr);
	);
	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}

void
catalog_open(Catalog* self)
{
	// prepare system schemas
	catalog_create_system_schema(self, "system", false);
	catalog_create_system_schema(self, "public", true);
}

void
catalog_close(Catalog* self)
{
	catalog_free(self);
}

Buf*
catalog_status(Catalog* self)
{
	// {}
	auto buf = buf_create();
	encode_obj(buf);

	int tables = 0;
	int tables_secondary_indexes = 0;
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		tables++;
		tables_secondary_indexes += (table->config->indexes_count - 1);
	}

	// schemas
	encode_raw(buf, "schemas", 7);
	encode_integer(buf, self->schema_mgr.mgr.list_count);

	// tables
	encode_raw(buf, "tables", 6);
	encode_integer(buf, tables);

	// indexes
	encode_raw(buf, "secondary_indexes", 17);
	encode_integer(buf, tables_secondary_indexes);

	encode_obj_end(buf);
	return buf;
}

bool
catalog_execute(Catalog* self, Tr* tr, uint8_t* op, int flags)
{
	// [op, ...]
	auto cmd = ddl_of(op);
	bool write = false;
	switch (cmd) {
	case DDL_SCHEMA_CREATE:
	{
		auto config = schema_op_create_read(op);
		defer(schema_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = schema_mgr_create(&self->schema_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_SCHEMA_DROP:
	{
		Str  name;
		bool cascade;
		schema_op_drop_read(op, &name, &cascade);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_schema_drop(self, tr, &name, cascade, if_exists);
		break;
	}
	case DDL_SCHEMA_RENAME:
	{
		Str name;
		Str name_new;
		schema_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_schema_rename(self, tr, &name, &name_new, if_exists);
		break;
	}
	case DDL_TABLE_CREATE:
	{
		auto config = table_op_create_read(op);
		defer(table_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_mgr_create(&self->table_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_TABLE_DROP:
	{
		Str schema;
		Str name;
		table_op_drop_read(op, &schema, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_drop(&self->table_mgr, tr, &schema, &name, if_exists);
		break;
	}
	case DDL_TABLE_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		table_op_rename_read(op, &schema, &name, &schema_new, &name_new);

		// ensure schema exists and not system
		auto ref = schema_mgr_find(&self->schema_mgr, &schema_new, true);
		if (! ref->config->create)
			error("system schema <%.*s> cannot be used to create objects",
			      str_size(&ref->config->name),
			      str_of(&ref->config->name));

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_rename(&self->table_mgr, tr, &schema, &name,
		                         &schema_new,
		                         &name_new, if_exists);
		break;
	}
	case DDL_TABLE_TRUNCATE:
	{
		Str schema;
		Str name;
		table_op_truncate_read(op, &schema, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_truncate(&self->table_mgr, tr, &schema, &name, if_exists);
		break;
	}
	case DDL_TABLE_SET_IDENTITY:
	{
		Str     schema;
		Str     name;
		int64_t value;
		table_op_set_identity_read(op, &schema, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_identity(&self->table_mgr, tr, &schema, &name,
		                               value, if_exists);
		break;
	}
	case DDL_TABLE_SET_UNLOGGED:
	{
		Str  schema;
		Str  name;
		bool value;
		table_op_set_unlogged_read(op, &schema, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_unlogged(&self->table_mgr, tr, &schema, &name,
		                               value, if_exists);
		break;
	}
	case DDL_TABLE_COLUMN_ADD:
	{
		Str  schema;
		Str  name;
		auto column = table_op_column_add_read(op, &schema, &name);
		defer(column_free, column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_not_exists = ddl_if_column_not_exists(flags);
		auto table_new = table_mgr_column_add(&self->table_mgr, tr, &schema, &name,
		                                      column,
		                                      if_exists,
		                                      if_column_not_exists);
		if (! table_new)
			break;

		// build new table with new column
		auto table = table_mgr_find(&self->table_mgr, &schema, &name, true);
		self->iface->build_column_add(self, table, table_new, column);
		write = true;
		break;
	}
	case DDL_TABLE_COLUMN_DROP:
	{
		Str schema;
		Str name;
		Str name_column;
		table_op_column_drop_read(op, &schema, &name, &name_column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		auto table_new = table_mgr_column_drop(&self->table_mgr, tr, &schema, &name,
		                                       &name_column,
		                                       if_exists,
		                                       if_column_exists);
		if (! table_new)
			break;

		// build new table with new column
		auto table = table_mgr_find(&self->table_mgr, &schema, &name, true);
		auto column = columns_find(&table->config->columns, &name_column);
		assert(column);
		self->iface->build_column_drop(self, table, table_new, column);
		write = true;
		break;
	}
	case DDL_TABLE_COLUMN_RENAME:
	{
		Str schema;
		Str name;
		Str name_column;
		Str name_column_new;
		table_op_column_set_read(op, &schema, &name, &name_column, &name_column_new);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_rename(&self->table_mgr, tr, &schema, &name, &name_column,
		                                &name_column_new,
		                                if_exists,
		                                if_column_exists);
		break;
	}
	case DDL_TABLE_COLUMN_SET_DEFAULT:
	case DDL_TABLE_COLUMN_SET_IDENTITY:
	case DDL_TABLE_COLUMN_SET_STORED:
	case DDL_TABLE_COLUMN_SET_RESOLVED:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value;
		table_op_column_set_read(op, &schema, &name, &name_column, &value);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_set(&self->table_mgr, tr, &schema, &name, &name_column,
		                             &value, cmd,
		                             if_exists,
		                             if_column_exists);
		break;
	}
	case DDL_INDEX_CREATE:
	{
		Str schema;
		Str name;
		auto config_pos = table_op_index_create_read(op, &schema, &name);

		// create and build index
		auto table  = table_mgr_find(&self->table_mgr, &schema, &name, true);
		auto config = index_config_read(table_columns(table), &config_pos);
		defer(index_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_index_create(table, tr, config, if_not_exists);
		if (write)
		{
			auto index = table_find_index(table, &config->name, true);
			self->iface->build_index(self, table, index);
		}
		break;
	}
	case DDL_INDEX_DROP:
	{
		Str schema;
		Str name;
		Str name_index;
		table_op_index_drop_read(op, &schema, &name, &name_index);

		auto table = table_mgr_find(&self->table_mgr, &schema, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_drop(table, tr, &name_index, if_exists);
		break;
	}
	case DDL_INDEX_RENAME:
	{
		Str schema;
		Str name;
		Str name_index;
		Str name_index_new;
		table_op_index_rename_read(op, &schema, &name, &name_index, &name_index_new);

		auto table = table_mgr_find(&self->table_mgr, &schema, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_rename(table, tr, &name_index, &name_index_new, if_exists);
		break;
	}
	default:
		error("unrecognized ddl operation id: %d", (int)cmd);
		break;
	}

	if (write)
	{
		log_persist_relation(&tr->log, op);
		return true;
	}
	return false;
}
