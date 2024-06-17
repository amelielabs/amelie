
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>

static void
table_index_delete(Table* table, IndexConfig* index)
{
	// remove index from partitions
	part_list_drop_index(&table->part_list, index);

	// remove index from table config
	table_config_del_index(table->config, index);

	// free
	index_config_free(index);
}

static void
table_index_create_commit(LogOp* op)
{
	buf_free(op->handle.data);
}

static void
table_index_create_abort(LogOp* op)
{
	auto table = table_of(op->handle.handle);
	IndexConfig* index = op->arg;
	table_index_delete(table, index);
	buf_free(op->handle.data);
}

bool
table_index_create(Table*       self,
                   Transaction* trx,
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
	keys_set_primary(&index->keys, table_keys(self));
	table_config_add_index(self->config, index);

	// save create index operation
	auto op = table_op_create_index(&self->config->schema,
	                                &self->config->name,
	                                 config);
	guard_buf(op);

	// update table
	log_handle(&trx->log, LOG_ALTER, LOG_INDEX_CREATE,
	           table_index_create_commit,
	           table_index_create_abort,
	           index,
	           &self->handle, op);
	unguard();

	// create index for each partition
	part_list_create_index(&self->part_list, index);
	return true;
}

static void
table_index_drop_commit(LogOp* op)
{
	auto table = table_of(op->handle.handle);
	IndexConfig* index = op->arg;
	table_index_delete(table, index);
	buf_free(op->handle.data);
}

static void
table_index_drop_abort(LogOp* op)
{
	buf_free(op->handle.data);
}

void
table_index_drop(Table*       self,
                 Transaction* trx,
                 Str*         name,
                 bool         if_exists)
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
	guard_buf(op);

	// update table
	log_handle(&trx->log, LOG_ALTER, LOG_INDEX_DROP,
	           table_index_drop_commit,
	           table_index_drop_abort,
	           index,
	           &self->handle, op);
	unguard();
}

static void
table_index_rename_commit(LogOp* op)
{
	buf_free(op->handle.data);
}

static void
table_index_rename_abort(LogOp* op)
{
	IndexConfig* index = op->arg;
	uint8_t* pos = op->handle.data->start;
	Str schema;
	Str name;
	Str index_name;
	Str index_name_new;
	table_op_rename_index_read(&pos, &schema, &name, &index_name, &index_name_new);
	index_config_set_name(index, &index_name);
	buf_free(op->handle.data);
}

void
table_index_rename(Table*       self,
                   Transaction* trx,
                   Str*         name,
                   Str*         name_new,
                   bool         if_exists)
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
	guard_buf(op);

	// update table
	log_handle(&trx->log, LOG_ALTER, LOG_INDEX_RENAME,
	           table_index_rename_commit,
	           table_index_rename_abort,
	           index,
	           &self->handle, op);
	unguard();

	// rename index
	index_config_set_name(index, name_new);
}
