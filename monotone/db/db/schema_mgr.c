
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_part.h>
#include <monotone_wal.h>
#include <monotone_db.h>

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
	guard(guard, schema_free, schema);

	// save create schema operation
	auto op = schema_op_create(config);

	// update schemas
	handle_mgr_write(&self->mgr, trx, LOG_SCHEMA_CREATE, &schema->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

static void
schema_mgr_drop_schema(SchemaMgr* self, Transaction* trx, Schema* schema)
{
	// save drop schema operation
	auto op = schema_op_drop(&schema->config->name);

	// drop schema by name
	Handle drop;
	handle_init(&drop);
	handle_set_schema(&drop, NULL);
	handle_set_name(&drop, &schema->config->name);

	handle_mgr_write(&self->mgr, trx, LOG_SCHEMA_DROP, &drop, op);
	buf_unpin(op);
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

	// drop schema object
	schema_mgr_drop_schema(self, trx, schema);
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

	// allocate new schema object
	auto update = schema_allocate(schema->config);
	guard(guard, schema_free, update);

	// set new name
	schema_config_set_name(update->config, name_new);

	// drop previus schema object
	schema_mgr_drop_schema(self, trx, schema);

	// save  schema operation
	auto op = schema_op_rename(name, name_new);

	// update schemas
	handle_mgr_write(&self->mgr, trx, LOG_SCHEMA_RENAME, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

void
schema_mgr_dump(SchemaMgr* self, Buf* buf)
{
	// exclude system objects
	int count = self->mgr.list_count - 2;

	// array
	encode_array(buf, count);
	list_foreach(&self->mgr.list)
	{
		auto schema = schema_of(list_at(Handle, link));
		if (schema->config->system)
			continue;
		schema_config_write(schema->config, buf);
	}
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
	auto buf = msg_create(MSG_OBJECT);
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		// {}
		auto schema = schema_of(list_at(Handle, link));
		schema_config_write(schema->config, buf);
	}
	msg_end(buf);
	return buf;
}
