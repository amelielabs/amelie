
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
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
	handle_mgr_write(&self->mgr, trx, LOG_CREATE_SCHEMA, &schema->handle, op);

	buf_unpin(op);
	unguard(&guard);
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

	// save drop table operation
	auto op = schema_op_drop(name);

	// drop by name
	Handle drop;
	handle_init(&drop);
	handle_set_name(&drop, name);

	// update mgr
	handle_mgr_write(&self->mgr, trx, LOG_DROP_SCHEMA, &drop, op);

	buf_unpin(op);
}

void
schema_mgr_dump(SchemaMgr* self, Buf* buf)
{
	// array
	encode_array(buf, self->mgr.list_count);
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
	// map
	encode_map(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		// name: {}
		auto schema = schema_of(list_at(Handle, link));
		encode_string(buf, &schema->config->name);
		schema_config_write(schema->config, buf);
	}
	msg_end(buf);
	return buf;
}
