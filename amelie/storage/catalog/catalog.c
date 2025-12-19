
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
	db_mgr_init(&self->db_mgr);
	table_mgr_init(&self->table_mgr, part_mgr);
	udf_mgr_init(&self->udf_mgr, iface->udf_free, iface_arg);
}

void
catalog_free(Catalog* self)
{
	udf_mgr_free(&self->udf_mgr);
	table_mgr_free(&self->table_mgr);
	db_mgr_free(&self->db_mgr);
}

static void
catalog_create_main_db(Catalog* self, const char* db)
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

		// create db
		Str name;
		str_init(&name);
		str_set_cstr(&name, db);
		auto config = db_config_allocate();
		defer(db_config_free, config);
		db_config_set_name(config, &name);
		db_config_set_system(config, true);
		db_mgr_create(&self->db_mgr, &tr, config, false);

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
	// prepare main db
	catalog_create_main_db(self, "main");
}

void
catalog_close(Catalog* self)
{
	catalog_free(self);
}

void
catalog_sync(Catalog* self)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			state_tsn_follow(part->heap.header->tsn_max);
		}
	}
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

	// databases
	encode_raw(buf, "databases", 9);
	encode_integer(buf, self->db_mgr.mgr.list_count);

	// tables
	encode_raw(buf, "tables", 6);
	encode_integer(buf, tables);

	// indexes
	encode_raw(buf, "secondary_indexes", 17);
	encode_integer(buf, tables_secondary_indexes);

	encode_obj_end(buf);
	return buf;
}

static void
catalog_validate_udfs(Catalog* self, Str* name)
{
	// validate udf dependencies on the relation
	list_foreach(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Relation, link));
		if (self->iface->udf_depends(udf, name))
			error("function '%.*s' depends on relation '%.*s",
			      str_size(udf->rel.name), str_of(udf->rel.name),
			      str_size(name), str_of(name));
	}
}

bool
catalog_execute(Catalog* self, Tr* tr, uint8_t* op, int flags)
{
	// [op, ...]
	auto cmd = ddl_of(op);
	bool write = false;
	switch (cmd) {
	case DDL_DB_CREATE:
	{
		auto config = db_op_create_read(op);
		defer(db_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = db_mgr_create(&self->db_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_DB_DROP:
	{
		Str  name;
		bool cascade;
		db_op_drop_read(op, &name, &cascade);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_db_drop(self, tr, &name, cascade, if_exists);
		break;
	}
	case DDL_DB_RENAME:
	{
		Str name;
		Str name_new;
		db_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_db_rename(self, tr, &name, &name_new, if_exists);
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
		Str db;
		Str name;
		table_op_drop_read(op, &db, &name);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_drop(&self->table_mgr, tr, &db, &name, if_exists);
		break;
	}
	case DDL_TABLE_RENAME:
	{
		Str db;
		Str name;
		Str db_new;
		Str name_new;
		table_op_rename_read(op, &db, &name, &db_new, &name_new);

		// ensure db exists
		db_mgr_find(&self->db_mgr, &db_new, true);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_rename(&self->table_mgr, tr, &db, &name,
		                         &db_new, &name_new, if_exists);
		break;
	}
	case DDL_TABLE_TRUNCATE:
	{
		Str db;
		Str name;
		table_op_truncate_read(op, &db, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_truncate(&self->table_mgr, tr, &db, &name, if_exists);
		break;
	}
	case DDL_TABLE_SET_IDENTITY:
	{
		Str     db;
		Str     name;
		int64_t value;
		table_op_set_identity_read(op, &db, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_identity(&self->table_mgr, tr, &db, &name,
		                               value, if_exists);
		break;
	}
	case DDL_TABLE_SET_UNLOGGED:
	{
		Str  db;
		Str  name;
		bool value;
		table_op_set_unlogged_read(op, &db, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_unlogged(&self->table_mgr, tr, &db, &name,
		                               value, if_exists);
		break;
	}
	case DDL_TABLE_COLUMN_ADD:
	{
		Str  db;
		Str  name;
		auto column = table_op_column_add_read(op, &db, &name);
		defer(column_free, column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_not_exists = ddl_if_column_not_exists(flags);
		auto table_new = table_mgr_column_add(&self->table_mgr, tr, &db, &name,
		                                      column,
		                                      if_exists,
		                                      if_column_not_exists);
		if (! table_new)
			break;

		// build new table with new column
		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		self->iface->build_column_add(self, table, table_new, column);
		write = true;
		break;
	}
	case DDL_TABLE_COLUMN_DROP:
	{
		Str db;
		Str name;
		Str name_column;
		table_op_column_drop_read(op, &db, &name, &name_column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		auto table_new = table_mgr_column_drop(&self->table_mgr, tr, &db, &name,
		                                       &name_column,
		                                       if_exists,
		                                       if_column_exists);
		if (! table_new)
			break;

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		// build new table with new column
		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto column = columns_find(&table->config->columns, &name_column);
		assert(column);
		self->iface->build_column_drop(self, table, table_new, column);
		write = true;
		break;
	}
	case DDL_TABLE_COLUMN_RENAME:
	{
		Str db;
		Str name;
		Str name_column;
		Str name_column_new;
		table_op_column_set_read(op, &db, &name, &name_column, &name_column_new);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_rename(&self->table_mgr, tr, &db, &name, &name_column,
		                                &name_column_new,
		                                if_exists,
		                                if_column_exists);
		break;
	}
	case DDL_TABLE_COLUMN_SET_DEFAULT:
	case DDL_TABLE_COLUMN_SET_STORED:
	case DDL_TABLE_COLUMN_SET_RESOLVED:
	{
		Str db;
		Str name;
		Str name_column;
		Str value;
		table_op_column_set_read(op, &db, &name, &name_column, &value);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_set(&self->table_mgr, tr, &db, &name, &name_column,
		                             &value, cmd,
		                             if_exists,
		                             if_column_exists);
		break;
	}
	case DDL_INDEX_CREATE:
	{
		Str  db;
		Str  name;
		auto config_pos = table_op_index_create_read(op, &db, &name);

		// create and build index
		auto table  = table_mgr_find(&self->table_mgr, &db, &name, true);
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
		Str db;
		Str name;
		Str name_index;
		table_op_index_drop_read(op, &db, &name, &name_index);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_drop(table, tr, &name_index, if_exists);
		break;
	}
	case DDL_INDEX_RENAME:
	{
		Str db;
		Str name;
		Str name_index;
		Str name_index_new;
		table_op_index_rename_read(op, &db, &name, &name_index, &name_index_new);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_rename(table, tr, &name_index, &name_index_new, if_exists);
		break;
	}
	case DDL_UDF_CREATE:
	{
		auto config = udf_op_create_read(op);
		defer(udf_config_free, config);

		// create or replace
		write = udf_mgr_create(&self->udf_mgr, tr, config, false);

		// compile on creation
		if (write)
		{
			auto udf = udf_mgr_find(&self->udf_mgr, &config->db, &config->name, true);
			self->iface->udf_compile(self, udf);
		}
		break;
	}
	case DDL_UDF_REPLACE:
	{
		auto config = udf_op_replace_read(op);
		defer(udf_config_free, config);

		// allow to replace udf only if it has identical arguments
		// and return type

		// create or replace
		auto udf = udf_mgr_find(&self->udf_mgr, &config->db, &config->name, false);
		if (udf)
		{
			// validate types
			udf_mgr_replace_validate(&self->udf_mgr, config, udf);

			// create and compile new udf
			auto replace = udf_allocate(config, self->udf_mgr.free, self->udf_mgr.free_arg);
			defer(udf_free, replace);
			self->iface->udf_compile(self, replace);

			udf_mgr_replace(&self->udf_mgr, tr, udf, replace);
			write = true;
			break;
		}

		// create
		write = udf_mgr_create(&self->udf_mgr, tr, config, false);

		// compile on creation
		if (write)
		{
			udf = udf_mgr_find(&self->udf_mgr, &config->db, &config->name, true);
			self->iface->udf_compile(self, udf);
		}
		break;
	}
	case DDL_UDF_DROP:
	{
		Str db;
		Str name;
		udf_op_drop_read(op, &db, &name);

		// ensure no other udfs depend on it
		catalog_validate_udfs(self, &name);

		auto if_exists = ddl_if_exists(flags);
		write = udf_mgr_drop(&self->udf_mgr, tr, &db, &name, if_exists);
		break;
	}
	case DDL_UDF_RENAME:
	{
		Str db;
		Str name;
		Str db_new;
		Str name_new;
		udf_op_rename_read(op, &db, &name, &db_new, &name_new);

		// ensure db exists and not system
		db_mgr_find(&self->db_mgr, &db_new, true);

		// ensure no other udfs depend on it
		catalog_validate_udfs(self, &name);

		auto if_exists = ddl_if_exists(flags);
		write = udf_mgr_rename(&self->udf_mgr, tr, &db, &name,
		                       &db_new, &name_new, if_exists);
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
