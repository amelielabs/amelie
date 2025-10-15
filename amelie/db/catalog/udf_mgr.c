
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
udf_mgr_init(UdfMgr* self, UdfFree free, void* free_arg)
{
	self->free     = free;
	self->free_arg = free_arg;
	relation_mgr_init(&self->mgr);
}

void
udf_mgr_free(UdfMgr* self)
{
	relation_mgr_free(&self->mgr);
}

bool
udf_mgr_create(UdfMgr*    self,
               Tr*        tr,
               UdfConfig* config,
               bool       if_not_exists)
{
	// make sure udf does not exists
	auto current = udf_mgr_find(self, &config->schema, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("udf '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create udf
	auto udf = udf_allocate(config, self->free, self->free_arg);
	relation_mgr_create(&self->mgr, tr, &udf->rel);
	return true;
}

void
udf_mgr_drop_of(UdfMgr* self, Tr* tr, Udf* udf)
{
	// drop udf by object
	relation_mgr_drop(&self->mgr, tr, &udf->rel);
}

bool
udf_mgr_drop(UdfMgr* self, Tr* tr, Str* schema, Str* name,
             bool if_exists)
{
	auto udf = udf_mgr_find(self, schema, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("udf '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	udf_mgr_drop_of(self, tr, udf);
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
	auto udf = udf_of(relation->relation);
	uint8_t* pos = relation->data;
	Str schema;
	Str name;
	json_read_string(&pos, &schema);
	json_read_string(&pos, &name);
	udf_config_set_schema(udf->config, &schema);
	udf_config_set_name(udf->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
udf_mgr_rename(UdfMgr* self,
               Tr*     tr,
               Str*    schema,
               Str*    name,
               Str*    schema_new,
               Str*    name_new,
               bool    if_exists)
{
	auto udf = udf_mgr_find(self, schema, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("udf '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// ensure new udf does not exists
	if (udf_mgr_find(self, schema_new, name_new, false))
		error("udf '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update udf
	log_relation(&tr->log, &rename_if, NULL, &udf->rel);

	// save previous name
	encode_string(&tr->log.data, &udf->config->schema);
	encode_string(&tr->log.data, &udf->config->name);

	// set new udf name
	if (! str_compare(&udf->config->schema, schema_new))
		udf_config_set_schema(udf->config, schema_new);

	if (! str_compare(&udf->config->name, name_new))
		udf_config_set_name(udf->config, name_new);

	return true;
}

void
udf_mgr_dump(UdfMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Relation, link));
		udf_config_write(udf->config, buf);
	}
	encode_array_end(buf);
}

Udf*
udf_mgr_find(UdfMgr* self, Str* schema, Str* name,
               bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, schema, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("udf '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return udf_of(relation);
}

Buf*
udf_mgr_list(UdfMgr* self, Str* schema, Str* name, bool extended)
{
	auto buf = buf_create();
	if (schema && name)
	{
		// show udf
		auto udf = udf_mgr_find(self, schema, name, false);
		if (udf) {
			if (extended)
				udf_config_write(udf->config, buf);
			else
				udf_config_write_compact(udf->config, buf);
		} else {
			encode_null(buf);
		}
		return buf;
	}

	// show udfs
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Relation, link));
		if (schema && !str_compare(&udf->config->schema, schema))
			continue;
		if (extended)
			udf_config_write(udf->config, buf);
		else
			udf_config_write_compact(udf->config, buf);
	}
	encode_array_end(buf);
	return buf;
}
