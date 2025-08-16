
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

static void
rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	uint8_t* pos = relation->data;
	Str schema;
	Str name;
	json_read_string(&pos, &schema);
	json_read_string(&pos, &name);
	table_config_set_schema(table->config, &schema);
	table_config_set_name(table->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
table_mgr_rename(TableMgr* self,
                 Tr*       tr,
                 Str*      schema,
                 Str*      name,
                 Str*      schema_new,
                 Str*      name_new,
                 bool      if_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// ensure new table does not exists
	if (table_mgr_find(self, schema_new, name_new, false))
		error("table '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update table
	log_relation(&tr->log, &rename_if, NULL, &table->rel);

	// save previous name
	encode_string(&tr->log.data, &table->config->schema);
	encode_string(&tr->log.data, &table->config->name);

	// set new table name
	if (! str_compare(&table->config->schema, schema_new))
		table_config_set_schema(table->config, schema_new);

	if (! str_compare(&table->config->name, name_new))
		table_config_set_name(table->config, name_new);

	return true;
}

static void
set_identity_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
set_identity_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	uint8_t* pos = relation->data;
	int64_t value;
	json_read_integer(&pos, &value);
	sequence_set(&table->seq, value);
}

static LogIf set_identity_if =
{
	.commit = set_identity_if_commit,
	.abort  = set_identity_if_abort
};

bool
table_mgr_set_identity(TableMgr* self,
                       Tr*       tr,
                       Str*      schema,
                       Str*      name,
                       int64_t   value,
                       bool      if_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// update table
	log_relation(&tr->log, &set_identity_if, NULL, &table->rel);

	// save previous sequence value
	encode_integer(&tr->log.data, sequence_get(&table->seq));

	// set new sequence
	sequence_set(&table->seq, value);
	return true;
}

static void
set_unlogged_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
set_unlogged_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	table_set_unlogged(table, !table->config->unlogged);
}

static LogIf set_unlogged_if =
{
	.commit = set_unlogged_if_commit,
	.abort  = set_unlogged_if_abort
};

bool
table_mgr_set_unlogged(TableMgr* self,
                       Tr*       tr,
                       Str*      schema,
                       Str*      name,
                       bool      value,
                       bool      if_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// update table
	log_relation(&tr->log, &set_unlogged_if, NULL, &table->rel);

	// set table and partitions as unlogged
	table_set_unlogged(table, value);
	return true;
}

static void
column_rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
column_rename_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	Column* column = op->iface_arg;
	uint8_t* pos = relation->data;
	Str name_column;
	json_read_string(&pos, &name_column);
	column_set_name(column, &name_column);
}

static LogIf column_rename_if =
{
	.commit = column_rename_if_commit,
	.abort  = column_rename_if_abort
};

bool
table_mgr_column_rename(TableMgr* self,
                        Tr*       tr,
                        Str*      schema,
                        Str*      name,
                        Str*      name_column,
                        Str*      name_column_new,
                        bool      if_exists,
                        bool      if_column_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// ensure column does not exists
	auto column = columns_find(&table->config->columns, name_column);
	if (! column)
	{
		if (! if_column_exists)
			error("table '%.*s': column '%.*s' not exists", str_size(name),
			      str_of(name),
			      str_size(name_column),
			      str_of(name_column));
		return false;
	}

	// ensure new column does not exists
	auto ref = columns_find(&table->config->columns, name_column_new);
	if (ref)
	{
		error("table '%.*s': column '%.*s' already exists", str_size(name),
		      str_of(name),
		      str_size(name_column_new),
		      str_of(name_column_new));
	}

	// update table
	log_relation(&tr->log, &column_rename_if, column, &table->rel);

	// save previous column name
	encode_string(&tr->log.data, &column->name);

	// set column name
	column_set_name(column, name_column_new);
	return true;
}

static void
column_add_if_commit(Log* self, LogOp* op)
{
	// swap former table with a new one
	TableMgr* mgr = op->iface_arg;
	auto relation = log_relation_of(self, op);
	auto table  = table_of(relation->relation);
	auto prev   = table_mgr_find(mgr, &table->config->schema,
	                             &table->config->name,
	                              true);
	relation_mgr_replace(&mgr->mgr, &prev->rel, &table->rel);

	// free previous table and unmap partitions
	table_free(prev);

	// remap new table partitions (partitions has same ids)
	part_list_map(&table->part_list);
	part_mgr_attach(table->part_mgr, &table->part_list);
}

static void
column_add_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	table_free(table);
}

static LogIf column_add_if =
{
	.commit = column_add_if_commit,
	.abort  = column_add_if_abort
};

Table*
table_mgr_column_add(TableMgr* self,
                     Tr*       tr,
                     Str*      schema,
                     Str*      name,
                     Column*   column,
                     bool      if_exists,
                     bool      if_not_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}

	// ensure column does not exists
	if (columns_find(&table->config->columns, &column->name))
	{
		if (! if_not_exists)
			error("table '%.*s': column '%.*s' already exists", str_size(name),
			      str_of(name),
			      str_size(&column->name),
			      str_of(&column->name));
		return NULL;
	}

	// physical column require a new table

	// allocate new table
	auto table_new = table_allocate(table->config, self->part_mgr);

	// add new column
	auto column_new = column_copy(column);
	columns_add(&table_new->config->columns, column_new);

	// update log (old table is still present)
	log_relation(&tr->log, &column_add_if, self,
	             &table_new->rel);

	table_open(table_new);
	return table_new;
}

Table*
table_mgr_column_drop(TableMgr* self,
                      Tr*       tr,
                      Str*      schema,
                      Str*      name,
                      Str*      name_column,
                      bool      if_exists,
                      bool      if_column_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}

	auto column = columns_find(&table->config->columns, name_column);
	if (! column)
	{
		if (! if_column_exists)
			error("table '%.*s': column '%.*s' not exists", str_size(name),
			      str_of(name),
			      str_size(name_column),
			      str_of(name_column));
		return NULL;
	}

	// ensure column currently not used as a key
	if (column->refs > 0)
		error("table '%.*s': column '%.*s' is a key", str_size(name),
		      str_of(name),
		      str_size(name_column),
		      str_of(name_column));

	// physical column require new table rebuild

	// allocate new table
	auto table_new = table_allocate(table->config, self->part_mgr);

	// delete and reorder columns and update keys
	auto column_new = columns_find(&table_new->config->columns, &column->name);
	columns_del(&table_new->config->columns, column_new);
	column_free(column_new);

	list_foreach(&table_new->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		keys_update(&config->keys);
	}

	// update log (old table is still present)
	log_relation(&tr->log, &column_add_if, self, &table_new->rel);

	table_open(table_new);
	return table_new;
}

static void
column_set_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
column_set_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	Column* column = op->iface_arg;
	uint8_t* pos = relation->data;
	auto cons = &column->constraints;
	constraints_free(cons);
	constraints_init(cons);
	constraints_read(cons, &pos);
	columns_sync(&table_of(relation->relation)->config->columns);
}

static LogIf column_set_if =
{
	.commit = column_set_if_commit,
	.abort  = column_set_if_abort
};

bool
table_mgr_column_set(TableMgr* self,
                     Tr*       tr,
                     Str*      schema,
                     Str*      name,
                     Str*      name_column,
                     Str*      value,
                     int       cmd,
                     bool      if_exists,
                     bool      if_column_exists)
{
	auto table = table_mgr_find(self, schema, name, false);
	if (! table)
	{
		if (! if_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	auto column = columns_find(&table->config->columns, name_column);
	if (! column)
	{
		if (! if_column_exists)
			error("table '%.*s': column '%.*s' not exists", str_size(name),
			      str_of(name),
			      str_size(name_column),
			      str_of(name_column));
		return false;
	}

	// update table
	log_relation(&tr->log, &column_set_if, column, &table->rel);

	// save previous constraints
	constraints_write(&column->constraints, &tr->log.data);

	switch (cmd) {
	case DDL_TABLE_COLUMN_SET_DEFAULT:
		constraints_set_default_str(&column->constraints, value);
		break;
	case DDL_TABLE_COLUMN_SET_IDENTITY:
		constraints_set_as_identity(&column->constraints, !str_empty(value));
		break;
	case DDL_TABLE_COLUMN_SET_STORED:
		constraints_set_as_stored(&column->constraints, value);
		break;
	case DDL_TABLE_COLUMN_SET_RESOLVED:
		constraints_set_as_resolved(&column->constraints, value);
		break;
	default:
		abort();
		break;
	}
	columns_sync(&table->config->columns);
	return true;
}
