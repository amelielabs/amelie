
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
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

static void
table_index_delete(Table* table, IndexConfig* index)
{
	// remove index from partitions
	part_list_index_drop(&table->part_list, index);

	// remove index from table config
	table_config_del_index(table->config, index);

	// free
	index_config_free(index);
}

static void
create_if_commit(Log* self, LogOp* op)
{
	buf_free(log_relation_of(self, op)->data);
}

static void
create_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	IndexConfig* index = op->iface_arg;
	table_index_delete(table, index);
	buf_free(relation->data);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

bool
table_index_create(Table*       self,
                   Tr*          tr,
                   IndexConfig* config,
                   bool         if_not_exists)
{
	auto index = table_find_index(self, &config->name, false);
	if (index)
	{
		if (! if_not_exists)
			error("table '%.*s' index '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// save index config copy to table config
	index = index_config_copy(config, &self->config->columns);
	keys_set_primary(&index->keys, false);
	table_config_add_index(self->config, index);

	// save create index operation
	auto op = table_op_create_index(&self->config->schema,
	                                &self->config->name,
	                                 config);

	// update table
	log_relation(&tr->log, CMD_INDEX_CREATE, &create_if,
	             index,
	             &self->rel, op);

	// create index for each partition
	part_list_index_create(&self->part_list, index);
	return true;
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto table = table_of(relation->relation);
	IndexConfig* index = op->iface_arg;
	table_index_delete(table, index);
	buf_free(relation->data);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	buf_free(log_relation_of(self, op)->data);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

void
table_index_drop(Table* self,
                 Tr*    tr,
                 Str*   name,
                 bool   if_exists)
{
	auto index = table_find_index(self, name, false);
	if (! index)
	{
		if (! if_exists)
			error("table '%.*s' index '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return;
	}

	// save drop index operation
	auto op = table_op_drop_index(&self->config->schema, &self->config->name, name);

	// update table
	log_relation(&tr->log, CMD_INDEX_DROP, &drop_if,
	             index,
	             &self->rel, op);
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	buf_free(log_relation_of(self, op)->data);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	IndexConfig* index = op->iface_arg;
	auto relation = log_relation_of(self, op);
	uint8_t* pos = relation->data->start;
	Str schema;
	Str name;
	Str index_name;
	Str index_name_new;
	table_op_rename_index_read(&pos, &schema, &name, &index_name, &index_name_new);
	index_config_set_name(index, &index_name);
	buf_free(relation->data);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

void
table_index_rename(Table* self,
                   Tr*    tr,
                   Str*   name,
                   Str*   name_new,
                   bool   if_exists)
{
	auto index = table_find_index(self, name, false);
	if (! index)
	{
		if (! if_exists)
			error("table '%.*s' index '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return;
	}

	// ensure new index not exists
	if (table_find_index(self, name_new, false))
		error("table '%.*s' index '%.*s': already exists",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name_new),
		      str_of(name_new));

	// save rename index operation
	auto op = table_op_rename_index(&self->config->schema, &self->config->name,
	                                name,
	                                name_new);

	// update table
	log_relation(&tr->log, CMD_INDEX_RENAME, &rename_if,
	             index,
	             &self->rel, op);

	// rename index
	index_config_set_name(index, name_new);
}
