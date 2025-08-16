
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
#include <amelie_runtime.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>

void
schema_mgr_init(SchemaMgr* self)
{
	relation_mgr_init(&self->mgr);
}

void
schema_mgr_free(SchemaMgr* self)
{
	relation_mgr_free(&self->mgr);
}

bool
schema_mgr_create(SchemaMgr*    self,
                  Tr*           tr,
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
		return false;
	}

	// allocate schema and init
	auto schema = schema_allocate(config);

	// update schemas
	relation_mgr_create(&self->mgr, tr, &schema->rel);
	return true;
}

bool
schema_mgr_drop(SchemaMgr* self,
                Tr*        tr,
                Str*       name,
                bool       if_exists)
{
	auto schema = schema_mgr_find(self, name, false);
	if (! schema)
	{
		if (! if_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (schema->config->system)
		error("schema '%.*s': system schema cannot be dropped", str_size(name),
		      str_of(name));

	// drop schema by object
	relation_mgr_drop(&self->mgr, tr, &schema->rel);
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
	auto relation = log_relation_of(self, op);
	auto mgr = schema_of(relation->relation);
	// set previous name
	uint8_t* pos = relation->data;
	Str name;
	json_read_string(&pos, &name);
	schema_config_set_name(mgr->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
schema_mgr_rename(SchemaMgr* self,
                  Tr*        tr,
                  Str*       name,
                  Str*       name_new,
                  bool       if_exists)
{
	auto schema = schema_mgr_find(self, name, false);
	if (! schema)
	{
		if (! if_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (schema->config->system)
		error("schema '%.*s': system schema cannot be altered", str_size(name),
		       str_of(name));

	// ensure new schema does not exists
	if (schema_mgr_find(self, name_new, false))
		error("schema '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update schema
	log_relation(&tr->log, &rename_if, NULL, &schema->rel);

	// save name for rollback
	encode_string(&tr->log.data, name);

	// set new name
	schema_config_set_name(schema->config, name_new);
	return true;
}

void
schema_mgr_dump(SchemaMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto schema = schema_of(list_at(Relation, link));
		if (schema->config->system)
			continue;
		schema_config_write(schema->config, buf);
	}
	encode_array_end(buf);
}

Schema*
schema_mgr_find(SchemaMgr* self, Str* name, bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, NULL, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("schema '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return schema_of(relation);
}

Buf*
schema_mgr_list(SchemaMgr* self, Str* name, bool extended)
{
	auto buf = buf_create();
	if (name)
	{
		auto schema = schema_mgr_find(self, name, false);
		if (schema)
		{
			if (extended)
				schema_config_write(schema->config, buf);
			else
				schema_config_write_compact(schema->config, buf);
		} else
		{
			encode_null(buf);
		}
	} else
	{
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto schema = schema_of(list_at(Relation, link));
			if (extended)
				schema_config_write(schema->config, buf);
			else
				schema_config_write_compact(schema->config, buf);
		}
		encode_array_end(buf);
	}
	return buf;
}
