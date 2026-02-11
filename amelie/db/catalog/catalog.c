
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>
#include <amelie_catalog.h>

void
catalog_init(Catalog*   self,
             CatalogIf* iface,
             void*      iface_arg,
             EngineIf*  iface_engine,
             void*      iface_engine_arg)
{
	self->iface = iface;
	self->iface_arg = iface_arg;
	storage_mgr_init(&self->storage_mgr);
	database_mgr_init(&self->db_mgr);
	table_mgr_init(&self->table_mgr, &self->storage_mgr,
	               iface_engine,
	               iface_engine_arg);
	udf_mgr_init(&self->udf_mgr, iface->udf_free, iface_arg);
}

void
catalog_free(Catalog* self)
{
	udf_mgr_free(&self->udf_mgr);
	table_mgr_free(&self->table_mgr);
	database_mgr_free(&self->db_mgr);
	storage_mgr_free(&self->storage_mgr);
}

static void
catalog_prepare(Catalog* self)
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

		Str name;
		str_init(&name);
		str_set_cstr(&name, "main");

		// create main storage
		auto storage_config = storage_config_allocate();
		defer(storage_config_free, storage_config);
		storage_config_set_name(storage_config, &name);
		storage_config_set_system(storage_config, true);
		storage_mgr_create(&self->storage_mgr, &tr, storage_config, false);

		// create main database
		auto db_config = database_config_allocate();
		defer(database_config_free, db_config);
		database_config_set_name(db_config, &name);
		database_config_set_system(db_config, true);
		database_mgr_create(&self->db_mgr, &tr, db_config, false);

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
catalog_open(Catalog* self, bool bootstrap)
{
	// prepare main tier and db
	catalog_prepare(self);

	// read catalog file and restore objects
	if (bootstrap)
		catalog_write(self);
	else
		catalog_read(self);
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

	// storages
	encode_raw(buf, "storages", 8);
	encode_integer(buf, self->storage_mgr.mgr.list_count);

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
	case DDL_STORAGE_CREATE:
	{
		auto config = storage_op_create_read(op);
		defer(storage_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = storage_mgr_create(&self->storage_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_STORAGE_DROP:
	{
		Str  name;
		storage_op_drop_read(op, &name);

		auto if_exists = ddl_if_exists(flags);
		write = storage_mgr_drop(&self->storage_mgr, tr, &name, if_exists);
		break;
	}
	case DDL_STORAGE_RENAME:
	{
		Str name;
		Str name_new;
		storage_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = storage_mgr_rename(&self->storage_mgr, tr, &name, &name_new, if_exists);
		break;
	}
	case DDL_DATABASE_CREATE:
	{
		auto config = database_op_create_read(op);
		defer(database_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = database_mgr_create(&self->db_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_DATABASE_DROP:
	{
		Str  name;
		bool cascade;
		database_op_drop_read(op, &name, &cascade);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_db_drop(self, tr, &name, cascade, if_exists);
		break;
	}
	case DDL_DATABASE_RENAME:
	{
		Str name;
		Str name_new;
		database_op_rename_read(op, &name, &name_new);

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
		database_mgr_find(&self->db_mgr, &db_new, true);

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
		write = table_mgr_column_add(&self->table_mgr, tr, &db, &name,
		                             column,
		                             if_exists,
		                             if_column_not_exists);
		break;
	}
	case DDL_TABLE_COLUMN_DROP:
	{
		Str db;
		Str name;
		Str name_column;
		table_op_column_drop_read(op, &db, &name, &name_column);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &name);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_drop(&self->table_mgr, tr, &db, &name,
		                              &name_column,
		                              if_exists,
		                              if_column_exists);
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
		// create index has different processing path and must be
		// done using the db_create_index()
		abort();
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
	case DDL_TIER_CREATE:
	{
		Str  db;
		Str  name;
		auto config_pos = table_op_tier_create_read(op, &db, &name);

		// create tier
		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto tier = tier_read(&config_pos);
		defer(tier_free, tier);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_tier_create(table, tr, tier, if_not_exists);
		break;
	}
	case DDL_TIER_DROP:
	{
		Str db;
		Str name;
		Str name_tier;
		table_op_tier_drop_read(op, &db, &name, &name_tier);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_tier_drop(table, tr, &name_tier, if_exists);
		break;
	}
	case DDL_TIER_RENAME:
	{
		Str db;
		Str name;
		Str name_tier;
		Str name_tier_new;
		table_op_tier_rename_read(op, &db, &name, &name_tier, &name_tier_new);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_tier_rename(table, tr, &name_tier, &name_tier_new, if_exists);
		break;
	}
	case DDL_TIER_STORAGE_ADD:
	{
		Str db;
		Str name;
		Str name_tier;
		Str name_storage;
		table_op_tier_storage_add_read(op, &db, &name, &name_tier, &name_storage);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		auto if_not_exists_storage = ddl_if_storage_not_exists(flags);
		write = table_tier_storage_add(table, tr, &name_tier, &name_storage,
		                               if_exists,
		                               if_not_exists_storage);
		break;
	}
	case DDL_TIER_SET:
	{
		Str     db;
		Str     name;
		int64_t mask;
		auto    config_pos = table_op_tier_set_read(op, &db, &name, &mask);

		// create tier
		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto tier = tier_read(&config_pos);
		defer(tier_free, tier);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_tier_set(table, tr, tier, mask, if_not_exists);
		break;
	}
	case DDL_TIER_STORAGE_DROP:
	{
		Str db;
		Str name;
		Str name_tier;
		Str name_storage;
		table_op_tier_storage_drop_read(op, &db, &name, &name_tier, &name_storage);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		auto if_exists_storage = ddl_if_storage_exists(flags);
		write = table_tier_storage_drop(table, tr, &name_tier, &name_storage,
		                                if_exists,
		                                if_exists_storage);
		break;
	}
	case DDL_TIER_STORAGE_PAUSE:
	{
		Str  db;
		Str  name;
		Str  name_tier;
		Str  name_storage;
		bool pause;
		table_op_tier_storage_pause_read(op, &db, &name, &name_tier, &name_storage, &pause);

		auto table = table_mgr_find(&self->table_mgr, &db, &name, true);
		auto if_exists = ddl_if_exists(flags);
		auto if_exists_storage = ddl_if_storage_exists(flags);
		write = table_tier_storage_pause(table, tr, &name_tier, &name_storage,
		                                 pause,
		                                 if_exists,
		                                 if_exists_storage);
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
			defer(relation_free, replace);
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
		database_mgr_find(&self->db_mgr, &db_new, true);

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
