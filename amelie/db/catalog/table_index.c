
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
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
table_index_delete(Table* table, IndexConfig* index)
{
	// remove index from partitions
	part_mgr_index_remove(&table->part_mgr, &index->name);

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
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	IndexConfig* index = op->iface_arg;
	table_index_delete(table, index);
}

static LogIf add_if =
{
	.commit = add_if_commit,
	.abort  = add_if_abort
};

void
table_index_add(Table* self, Tr* tr, IndexConfig* config)
{
	table_config_index_add(self->config, config);

	// update table
	log_relation(&tr->log, &add_if, index, &self->rel);

	// use separate log command for create
	// index processing
	log_last(&tr->log)->cmd = CMD_DDL_CREATE_INDEX;
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
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
table_index_drop(Table* self,
                 Tr*    tr,
                 Str*   name,
                 bool   if_exists)
{
	auto index = table_index_find(self, name, false);
	if (! index)
	{
		if (! if_exists)
			error("table '%.*s' index '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// update table
	log_relation(&tr->log, &drop_if, index, &self->rel);
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
	IndexConfig* index = op->iface_arg;
	auto relation = log_relation_of(self, op);
	uint8_t* pos = relation->data;
	Str index_name;
	json_read_string(&pos, &index_name);
	index_config_set_name(index, &index_name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
table_index_rename(Table* self,
                   Tr*    tr,
                   Str*   name,
                   Str*   name_new,
                   bool   if_exists)
{
	auto index = table_index_find(self, name, false);
	if (! index)
	{
		if (! if_exists)
			error("table '%.*s' index '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// ensure new index not exists
	if (table_index_find(self, name_new, false))
		error("table '%.*s' index '%.*s': already exists",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name_new),
		      str_of(name_new));

	// update table
	log_relation(&tr->log, &rename_if, index, &self->rel);

	// save previous name
	encode_string(&tr->log.data, &index->name);

	// rename index
	index_config_set_name(index, name_new);
	return true;
}

Buf*
table_index_list(Table* self, Str* ref, int flags)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show index name on table
	if (ref)
	{
		auto index = table_index_find(self, ref, false);
		if (! index)
			encode_null(buf);
		else
			index_config_write(index, buf, flags);
		return buf;
	}

	// show indexes on table
	encode_array(buf);
	list_foreach(&self->config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		index_config_write(index, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}

hot IndexConfig*
table_index_find(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		if (str_compare_case(&config->name, name))
			return config;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}
