
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

void
schema_mgr_init(SchemaMgr* self)
{
	handle_mgr_init(&self->mgr);
}

void
schema_mgr_free(SchemaMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
schema_mgr_create(SchemaMgr*    self,
                  Transaction*  trx,
                  SchemaConfig* config,
                  bool          if_not_exists)
{
	// make sure schema does not exists
	auto current = schema_mgr_find(self, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("schema '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	// allocate schema and init
	auto schema = schema_allocate(config);
	guard(schema_free, schema);

	// save create schema operation
	auto op = schema_op_create(config);
	guard_buf(op);

	// update schemas
	handle_mgr_create(&self->mgr, trx, LOG_SCHEMA_CREATE,
	                  &schema->handle, op);

	unguard();
	unguard();
}

void
schema_mgr_drop(SchemaMgr*   self,
                Transaction* trx,
                Str*         name,
                bool         if_exists)
{
	auto schema = schema_mgr_find(self, name, false);
	if (! schema)
	{
		if (! if_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	if (schema->config->system)
		error("schema '%.*s': system schema cannot be dropped", str_size(name),
		      str_of(name));

	// save drop schema operation
	auto op = schema_op_drop(&schema->config->name);
	guard_buf(op);

	// drop schema by object
	handle_mgr_drop(&self->mgr, trx, LOG_SCHEMA_DROP, &schema->handle, op);
	unguard();
}

static void
schema_mgr_rename_commit(LogOp* op)
{
	buf_free(op->handle.data);
}

static void
schema_mgr_rename_abort(LogOp* op)
{
	auto self = schema_of(op->handle.handle);
	// set previous name
	uint8_t* pos = op->handle.data->start;
	Str name;
	Str name_new;
	schema_op_rename_read(&pos, &name, &name_new);
	schema_config_set_name(self->config, &name);
	buf_free(op->handle.data);
}

void
schema_mgr_rename(SchemaMgr*   self,
                  Transaction* trx,
                  Str*         name,
                  Str*         name_new,
                  bool         if_exists)
{
	auto schema = schema_mgr_find(self, name, false);
	if (! schema)
	{
		if (! if_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	if (schema->config->system)
		error("schema '%.*s': system schema cannot be altered", str_size(name),
		       str_of(name));

	// ensure new schema does not exists
	if (schema_mgr_find(self, name_new, false))
		error("schema '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// save schema operation
	auto op = schema_op_rename(name, name_new);
	guard_buf(op);

	// update schema
	log_handle(&trx->log, LOG_SCHEMA_RENAME,
	           schema_mgr_rename_commit,
	           schema_mgr_rename_abort,
	           NULL,
	           &schema->handle, op);
	unguard();

	// set new name
	schema_config_set_name(schema->config, name_new);
}

void
schema_mgr_dump(SchemaMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto schema = schema_of(list_at(Handle, link));
		if (schema->config->system)
			continue;
		schema_config_write(schema->config, buf);
	}
	encode_array_end(buf);
}

Schema*
schema_mgr_find(SchemaMgr* self, Str* name, bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, NULL, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return schema_of(handle);
}

Buf*
schema_mgr_list(SchemaMgr* self)
{
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto schema = schema_of(list_at(Handle, link));
		schema_config_write(schema->config, buf);
	}
	encode_array_end(buf);
	return buf_end(buf);
}
