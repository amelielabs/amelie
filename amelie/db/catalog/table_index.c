
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
table_index_delete(Table* table, IndexConfig* index)
{
	// remove index from partitions
	parts_index_remove(&table->parts, &index->name);

	// remove index from table config
	table_config_index_remove(table->config, index);

	// free
	index_config_free(index);
}

static void
add_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
add_if_abort(Log* self, LogOp* op)
{
	unused(self);
	auto table = table_of(op->rel);
	IndexConfig* index = op->iface_arg;

	// restore key column constraints (reset not null constraint)
	uint8_t* pos = log_data_of(self, op);
	list_foreach(&index->keys.list)
	{
		auto key = list_at(Key, link);
		auto column = key->column;
		auto cons = &column->constraints;
		constraints_free(cons);
		constraints_init(cons);
		constraints_read(cons, &pos);
		columns_sync(&table_of(op->rel)->config->columns);
	}

	table_index_delete(table, index);
}

static LogIf add_if =
{
	.commit = add_if_commit,
	.abort  = add_if_abort
};

void
table_index_add(Catalog* self, Table* table, Tr* tr, IndexConfig* config)
{
	// only owner or superuser
	unused(self);
	check_ownership(tr, &table->rel);

	table_config_index_add(table->config, config);

	// update table
	log_ddl(&tr->log, &add_if, index, &table->rel);

	// create not null constraints on the columns
	list_foreach(&config->keys.list)
	{
		auto key = list_at(Key, link);
		auto column = key->column;
		constraints_write(&column->constraints, &tr->log.data, 0);
		constraints_set_not_null(&column->constraints, true);
	}

	// use separate log command for create
	// index processing
	log_last(&tr->log)->cmd = LOG_DDL;
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	unused(self);
	auto table = table_of(op->rel);
	IndexConfig* index = op->iface_arg;
	table_index_delete(table, index);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

bool
table_index_drop(Catalog* self,
                 Table*   table,
                 Tr*      tr,
                 Str*     name,
                 bool     if_exists)
{
	// only owner or superuser
	check_ownership(tr, &table->rel);

	auto index = table_index_find(table, name, false);
	if (! index)
	{
		if (! if_exists)
			error("table '{str}' index '{str}': not exists",
			      &table->config->name, name);
		return false;
	}

	// do not allow primary index drop
	if (index == table_primary(table))
		error("table '{str}' index '{str}': primary index cannot be dropped",
		      &table->config->name, name);

	// ensure no strict dependecies on udfs
	catalog_deps_validate_udf(self, &table->rel, true);

	// update table
	log_ddl(&tr->log, &drop_if, index, &table->rel);
	return true;
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str index_name;
	unpack_str(&pos, &index_name);

	IndexConfig* index = op->iface_arg;
	index_config_set_name(index, &index_name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
table_index_rename(Catalog* self,
                   Table*   table,
                   Tr*      tr,
                   Str*     name,
                   Str*     name_new,
                   bool     if_exists)
{
	// only owner or superuser
	check_ownership(tr, &table->rel);

	auto index = table_index_find(table, name, false);
	if (! index)
	{
		if (! if_exists)
			error("table '{str}' index '{str}': not exists",
			      &table->config->name, name);
		return false;
	}

	// ensure new index not exists
	if (table_index_find(table, name_new, false))
		error("table '{str}' index '{str}': already exists",
		      &table->config->name, name_new);

	// primary index cannot be renamed
	if (index == table_primary(table))
		error("table '{str}' index '{str}': primary index cannot be renamed",
		      &table->config->name, name);

	// ensure no strict dependecies
	catalog_deps_validate(self, &table->rel, true);

	// update table
	log_ddl(&tr->log, &rename_if, index, &table->rel);

	// save previous name
	encode_str(&tr->log.data, &index->name);

	// rename index
	index_config_set_name(index, name_new);
	return true;
}

void
table_index_list(Table* self, Buf* buf, Str* ref, int flags)
{
	// show index name on table
	if (ref)
	{
		auto index = table_index_find(self, ref, false);
		if (! index)
			encode_null(buf);
		else
			index_config_write(index, buf, flags);
		return;
	}

	// show indexes on table
	encode_array(buf);
	list_foreach(&self->config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		index_config_write(index, buf, flags);
	}
	encode_array_end(buf);
}

hot IndexConfig*
table_index_find(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		if (str_compare(&config->name, name))
			return config;
	}
	if (error_if_not_exists)
		error("index '{str}': not exists", name);
	return NULL;
}
