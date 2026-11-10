
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
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

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
		write = db_create(self, tr, config, if_not_exists);
		break;
	}
	case DDL_DB_DROP:
	{
		Str name;
		db_op_drop_read(op, &name);

		auto if_exists = ddl_if_exists(flags);
		write = db_drop(self, tr, &name, if_exists);
		break;
	}
	case DDL_DB_RENAME:
	{
		Str name;
		Str name_new;
		db_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = db_rename(self, tr, &name, &name_new, if_exists);
		break;
	}
	case DDL_DB_DESCRIBE:
	{
		Str name;
		Str description;
		db_op_describe_read(op, &name, &description);

		auto if_exists = ddl_if_exists(flags);
		write = db_describe(self, tr, &name, &description, if_exists);
		break;
	}
	case DDL_DB_REVOKE_TOKEN:
	{
		Str name;
		Str revoked_at;
		db_op_revoke_token_read(op, &name, &revoked_at);

		auto if_exists = ddl_if_exists(flags);
		write = db_revoke_token(self, tr, &name, &revoked_at, if_exists);
		break;
	}
	case DDL_DB_LIMIT_SET:
	{
		Str    name;
		Limits limits;
		limits_init(&limits);
		db_op_limit_set_read(op, &name, &limits);

		auto if_exists = ddl_if_exists(flags);
		write = db_limit_set(self, tr, &name, &limits, if_exists);
		break;
	}
	case DDL_DB_LIMIT_UNSET:
	{
		Str      name;
		uint64_t mask;
		db_op_limit_unset_read(op, &name, &mask);

		auto if_exists = ddl_if_exists(flags);
		write = db_limit_unset(self, tr, &name, mask, if_exists);
		break;
	}
	case DDL_DROP:
	{
		Str  name;
		bool cascade;
		auto type = rel_op_drop_read(op, &name, &cascade);

		auto if_exists = ddl_if_exists(flags);
		write = catalog_drop(self, tr, type, &name, if_exists, cascade);
		break;
	}
	case DDL_RENAME:
	{
		Str  name;
		Str  name_new;
		auto type = rel_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = catalog_rename(self, tr, type, &name, &name_new, if_exists);
		break;
	}
	case DDL_DESCRIBE:
	{
		Str  name;
		Str  description;
		auto type = rel_op_describe_read(op, &name, &description);

		auto if_exists = ddl_if_exists(flags);
		write = catalog_describe(self, tr, type, &name, &description, if_exists);
		break;
	}
	case DDL_TABLE_CREATE:
	{
		auto config = table_op_create_read(op);
		defer(table_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_create(self, tr, config, if_not_exists);
		break;
	}
	case DDL_TABLE_TRUNCATE:
	{
		Str name;
		table_op_truncate_read(op, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_truncate(self, tr, &name, if_exists);
		break;
	}
	case DDL_TABLE_COLUMN_ADD:
	{
		Str  name;
		auto column = table_op_column_add_read(op, &name);
		defer(column_free, column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_not_exists = ddl_if_column_not_exists(flags);
		write = table_column_add(self, tr, &name,
		                         column,
		                         if_exists,
		                         if_column_not_exists);
		break;
	}
	case DDL_TABLE_COLUMN_DROP:
	{
		Str name;
		Str name_column;
		table_op_column_drop_read(op, &name, &name_column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_column_drop(self, tr, &name,
		                          &name_column,
		                          if_exists,
		                          if_column_exists);
		break;
	}
	case DDL_TABLE_COLUMN_RENAME:
	{
		Str name;
		Str name_column;
		Str name_column_new;
		table_op_column_set_read(op, &name, &name_column, &name_column_new);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_column_rename(self, tr, &name, &name_column,
		                            &name_column_new,
		                            if_exists,
		                            if_column_exists);
		break;
	}
	case DDL_TABLE_COLUMN_SET_DEFAULT:
	{
		Str name;
		Str name_column;
		Str value;
		table_op_column_set_read(op, &name, &name_column, &value);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_column_set(self, tr, &name, &name_column,
		                         &value, cmd,
		                         if_exists,
		                         if_column_exists);
		break;
	}
	case DDL_TABLE_RETENTION:
	{
		Str name;
		int64_t value;
		table_op_retention_read(op, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_retention(self, tr, &name, value, if_exists);
		break;
	}
	case DDL_INDEX_CREATE:
	{
		Str  name;
		auto pos = table_op_index_create_read(op, &name);

		auto table  = catalog_find_table(self, &name, true);
		auto config = index_config_read(table_columns(table), &pos);
		errdefer(index_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_index_create(self, tr, table, config, if_not_exists);
		break;
	}
	case DDL_INDEX_DROP:
	{
		Str name;
		Str name_index;
		table_op_index_drop_read(op, &name, &name_index);

		auto table = catalog_find_table(self, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_drop(self, table, tr, &name_index, if_exists);
		break;
	}
	case DDL_INDEX_RENAME:
	{
		Str name;
		Str name_index;
		Str name_index_new;
		table_op_index_rename_read(op, &name, &name_index, &name_index_new);

		auto table = catalog_find_table(self, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_rename(self, table, tr, &name_index, &name_index_new, if_exists);
		break;
	}
	case DDL_INDEX_PRIMARY:
	{
		Str  name;
		Str  name_index;
		bool pk;
		table_op_index_primary_read(op, &name, &name_index, &pk);

		auto table = catalog_find_table(self, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_primary(self, table, tr, &name_index, pk, if_exists);
		break;
	}
	case DDL_UDF_CREATE:
	{
		bool replace;
		auto config = udf_op_create_read(op, &replace);
		defer(udf_config_free, config);

		// create or replace
		write = udf_create(self, tr, config, replace);
		break;
	}
	default:
		error("unrecognized ddl operation id: {d}", (int)cmd);
		break;
	}

	return write;
}
