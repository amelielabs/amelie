
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
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

int
ddl_schema_create(Buf* self, SchemaConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_SCHEMA_CREATE);
	schema_config_write(config, self);
	encode_array_end(self);
	return offset;
}

int
ddl_schema_drop(Buf* self, Str* name) 
{
	// [op, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_SCHEMA_DROP);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

int
ddl_schema_rename(Buf* self, Str* name, Str* name_new)
{
	// [op, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_SCHEMA_RENAME);
	encode_string(self, name);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

int
ddl_table_create(Buf* self, TableConfig* config) 
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_CREATE);
	table_config_write(config, self);
	encode_array_end(self);
	return offset;
}

int
ddl_table_drop(Buf* self, Str* schema, Str* name) 
{
	// [op, schema, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

int
ddl_table_rename(Buf* self, Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	// [op, schema, name, schema_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, schema_new);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

int
ddl_table_truncate(Buf* self, Str* schema, Str* name)
{
	// [op, schema, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_TRUNCATE);
	encode_string(self, schema);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

int
ddl_table_set_unlogged(Buf* self, Str* schema, Str* name, bool value)
{
	// [op, schema, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_SET_UNLOGGED);
	encode_string(self, schema);
	encode_string(self, name);
	encode_bool(self, value);
	encode_array_end(self);
	return offset;
}

int
ddl_table_column_add(Buf* self, Str* schema, Str* name, Column* column)
{
	// [op, schema, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_ADD);
	encode_string(self, schema);
	encode_string(self, name);
	column_write(column, self);
	encode_array_end(self);
	return offset;
}

int
ddl_table_column_drop(Buf* self, Str* schema, Str* name, Str* name_column)
{
	// [op, schema, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_column);
	encode_array_end(self);
	return offset;
}

int
ddl_table_column_set(Buf* self, int op, Str* schema, Str* name, Str* column,
                     Str* value_prev, Str* value)
{
	// [op, schema, name, column, value_prev, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, op);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, column);
	encode_string(self, value_prev);
	encode_string(self, value);
	encode_array_end(self);
	return offset;
}

int
ddl_table_column_rename(Buf* self, Str* schema, Str* name,
                        Str* name_column,
                        Str* name_column_new)
{
	// [op, schema, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_column);
	encode_string(self, name_column_new);
	encode_array_end(self);
	return offset;
}

int
ddl_index_create(Buf* self, Str* schema, Str* name, IndexConfig* config)
{
	// [op, schema, name, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_CREATE);
	encode_string(self, schema);
	encode_string(self, name);
	index_config_write(config, self);
	encode_array_end(self);
	return offset;
}

int
ddl_index_drop(Buf* self, Str* schema, Str* name, Str* name_index)
{
	// [op, schema, name, name_index]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_index);
	encode_array_end(self);
	return offset;
}

int
ddl_index_rename(Buf* self, Str* schema, Str* name, Str* name_index, Str* name_index_new)
{
	// [op, schema, name, name_index, name_index_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_index);
	encode_string(self, name_index_new);
	encode_array_end(self);
	return offset;
}

int
ddl_execute(Db* db, Tr* tr, uint8_t** pos)
{
	// [op, ...]
	json_read_array(pos);

	int64_t op;
	json_read_integer(pos, &op);
	switch (op) {
	// schema
	case DDL_SCHEMA_CREATE:
	{
		auto config = schema_config_read(pos);
		defer(schema_config_free, config);
		schema_mgr_create(&db->schema_mgr, tr, config, false);
		break;
	}
	case DDL_SCHEMA_DROP:
	{
		Str name;
		json_read_string(pos, &name);
		schema_mgr_drop(&db->schema_mgr, tr, &name, true);
		break;
	}
	case DDL_SCHEMA_RENAME:
	{
		Str name;
		Str name_new;
		json_read_string(pos, &name);
		json_read_string(pos, &name_new);
		schema_mgr_rename(&db->schema_mgr, tr, &name, &name_new, true);
		break;
	}

	// table
	case DDL_TABLE_CREATE:
	{
		auto config = table_config_read(pos);
		defer(table_config_free, config);
		table_mgr_create(&db->table_mgr, tr, config, false);
		break;
	}
	case DDL_TABLE_DROP:
	{
		Str schema;
		Str name;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		table_mgr_drop(&db->table_mgr, tr, &schema, &name, true);
		break;
	}
	case DDL_TABLE_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &schema_new);
		json_read_string(pos, &name_new);
		table_mgr_rename(&db->table_mgr, tr, &schema, &name,
		                 &schema_new, &name_new, true);
		break;
	}
	case DDL_TABLE_TRUNCATE:
	{
		Str schema;
		Str name;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		table_mgr_truncate(&db->table_mgr, tr, &schema, &name, true);
		break;
	}
	case DDL_TABLE_SET_UNLOGGED:
	{
		Str  schema;
		Str  name;
		bool value;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_bool(pos, &value);
		table_mgr_set_unlogged(&db->table_mgr, tr, &schema, &name, value, true);
		break;
	}
	case DDL_TABLE_COLUMN_ADD:
	{
		Str schema;
		Str name;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		auto column = column_read(pos);
		defer(column_free, column);

		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto table_new = table_mgr_column_add(&db->table_mgr, tr, &schema, &name,
		                                      column, true);
		if (! table_new)
			break;

		// build new table with new column
		db->iface->build_column_add(db, table, table_new, column);
		break;
	}
	case DDL_TABLE_COLUMN_DROP:
	{
		Str schema;
		Str name;
		Str name_column;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_column);

		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto table_new = table_mgr_column_drop(&db->table_mgr, tr, &schema, &name,
		                                       &name_column, true);
		if (! table_new)
			break;
		auto column = columns_find(&table->config->columns, &name_column);
		assert(column);

		// build new table without column
		db->iface->build_column_drop(db, table, table_new, column);
		break;
	}
	case DDL_TABLE_COLUMN_SET_DEFAULT:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_column);
		json_read_string(pos, &value_prev);
		json_read_string(pos, &value);
		table_mgr_column_set_default(&db->table_mgr, tr, &schema, &name,
		                             &name_column, &value, true);
		break;
	}
	case DDL_TABLE_COLUMN_SET_IDENTITY:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_column);
		json_read_string(pos, &value_prev);
		json_read_string(pos, &value);
		table_mgr_column_set_identity(&db->table_mgr, tr, &schema, &name,
		                              &name_column, &value, true);
		break;
	}
	case DDL_TABLE_COLUMN_SET_STORED:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_column);
		json_read_string(pos, &value_prev);
		json_read_string(pos, &value);
		table_mgr_column_set_stored(&db->table_mgr, tr, &schema, &name,
		                            &name_column, &value, true);
		break;
	}
	case DDL_TABLE_COLUMN_SET_RESOLVED:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_column);
		json_read_string(pos, &value_prev);
		json_read_string(pos, &value);
		table_mgr_column_set_resolved(&db->table_mgr, tr, &schema, &name,
		                              &name_column, &value, true);
		break;
	}
	case DDL_TABLE_COLUMN_RENAME:
	{
		Str schema;
		Str name;
		Str name_column;
		Str name_column_new;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_column);
		json_read_string(pos, &name_column_new);
		table_mgr_column_rename(&db->table_mgr, tr, &schema, &name,
		                        &name_column,
		                        &name_column_new, true);
		break;
	}

	// index
	case DDL_INDEX_CREATE:
	{
		Str schema;
		Str name;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		auto table  = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto config = index_config_read(table_columns(table), pos);
		defer(index_config_free, config);
		table_index_create(table, tr, config, false);

		// build index
		auto index = table_find_index(table, &config->name, true);
		db->iface->build_index(db, table, index);
		break;
	}
	case DDL_INDEX_DROP:
	{
		Str schema;
		Str name;
		Str name_index;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_index);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		table_index_drop(table, tr, &name_index, false);
		break;
	}
	case DDL_INDEX_RENAME:
	{
		Str schema;
		Str name;
		Str name_index;
		Str name_index_new;
		json_read_string(pos, &schema);
		json_read_string(pos, &name);
		json_read_string(pos, &name_index);
		json_read_string(pos, &name_index_new);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		table_index_rename(table, tr, &name_index, &name_index_new, false);
		break;
	}
	default:
		error("unrecognized ddl operation id: %d", (int)op);
	}

	json_read_array_end(pos);
	return op;
}
