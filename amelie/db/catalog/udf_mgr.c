
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
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>
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
	auto current = udf_mgr_find(self, &config->db, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("function '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create udf
	auto udf = udf_allocate(config, self->free, self->free_arg);
	relation_mgr_create(&self->mgr, tr, &udf->rel);
	return true;
}

static void
replace_if_commit(Log* self, LogOp* op)
{
	// free previous config and data by constructing
	// temprorary udf object
	UdfMgr* mgr = op->iface_arg;
	auto relation = log_relation_of(self, op);
	auto data = (void**)relation->data;
	auto tmp = udf_allocate_as(data[0], data[1], mgr->free, mgr->free_arg);
	udf_free(tmp);
}

static void
replace_if_abort(Log* self, LogOp* op)
{
	// free current config and data by constructing
	// temprorary udf object
	UdfMgr* mgr = op->iface_arg;
	auto relation = log_relation_of(self, op);
	auto udf = udf_of(relation->relation);

	auto tmp = udf_allocate_as(udf->config, udf->data, mgr->free, mgr->free_arg);
	udf_free(tmp);

	// set previous config and data
	auto data = (void**)relation->data;
	udf->config = data[0];
	udf->data   = data[1];

	// update relation data
	relation_set_db(&udf->rel, &udf->config->db);
	relation_set_name(&udf->rel, &udf->config->name);
}

static LogIf replace_if =
{
	.commit = replace_if_commit,
	.abort  = replace_if_abort
};

void
udf_mgr_replace_validate(UdfMgr* self, UdfConfig* config, Udf* udf)
{
	unused(self);

	// validate arguments
	if (! columns_compare(&udf->config->args, &config->args))
		error("function replacement '%.*s' arguments mismatch",
		      str_size(&config->name), str_of(&config->name));

	// validate return type
	if (udf->config->type != config->type)
		error("function replacement '%.*s' return type mismatch",
		      str_size(&config->name), str_of(&config->name));

	// validate returning columns
	if (! columns_compare(&udf->config->returning, &config->returning))
		error("function replacement '%.*s' returning columns mismatch",
		      str_size(&config->name), str_of(&config->name));
}

void
udf_mgr_replace(UdfMgr* self,
                Tr*     tr,
                Udf*    udf,
                Udf*    udf_new)
{
	// save previous udf config and data
	assert(udf->data);
	assert(udf_new->data);

	// update log
	log_relation(&tr->log, &replace_if, self, &udf->rel);

	buf_write(&tr->log.data, &udf->config, sizeof(void**));
	buf_write(&tr->log.data, &udf->data, sizeof(void**));

	// swap udf data and config
	udf->config = udf_new->config;
	udf->data   = udf_new->data;
	udf_new->config = NULL;
	udf_new->data   = NULL;

	// update relation data
	relation_set_db(&udf->rel, &udf->config->db);
	relation_set_name(&udf->rel, &udf->config->name);
}

void
udf_mgr_drop_of(UdfMgr* self, Tr* tr, Udf* udf)
{
	// drop udf by object
	relation_mgr_drop(&self->mgr, tr, &udf->rel);
}

bool
udf_mgr_drop(UdfMgr* self, Tr* tr, Str* db, Str* name,
             bool    if_exists)
{
	auto udf = udf_mgr_find(self, db, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
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
	Str db;
	Str name;
	json_read_string(&pos, &db);
	json_read_string(&pos, &name);
	udf_config_set_db(udf->config, &db);
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
               Str*    db,
               Str*    name,
               Str*    db_new,
               Str*    name_new,
               bool    if_exists)
{
	auto udf = udf_mgr_find(self, db, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// ensure new udf does not exists
	if (udf_mgr_find(self, db_new, name_new, false))
		error("function '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update udf
	log_relation(&tr->log, &rename_if, NULL, &udf->rel);

	// save previous name
	encode_string(&tr->log.data, &udf->config->db);
	encode_string(&tr->log.data, &udf->config->name);

	// set new name
	if (! str_compare_case(&udf->config->db, db_new))
		udf_config_set_db(udf->config, db_new);

	if (! str_compare_case(&udf->config->name, name_new))
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
udf_mgr_find(UdfMgr* self, Str* db, Str* name,
             bool    error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, db, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return udf_of(relation);
}

Buf*
udf_mgr_list(UdfMgr* self, Str* db, Str* name, bool extended)
{
	auto buf = buf_create();
	if (db && name)
	{
		// show udf
		auto udf = udf_mgr_find(self, db, name, false);
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
		if (db && !str_compare_case(&udf->config->db, db))
			continue;
		if (extended)
			udf_config_write(udf->config, buf);
		else
			udf_config_write_compact(udf->config, buf);
	}
	encode_array_end(buf);
	return buf;
}
