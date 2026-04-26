
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
udf_mgr_init(UdfMgr* self, UdfFree free, void* free_arg)
{
	self->free     = free;
	self->free_arg = free_arg;
	rel_mgr_init(&self->mgr);
}

void
udf_mgr_free(UdfMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
udf_mgr_create(UdfMgr*    self,
               Tr*        tr,
               UdfConfig* config,
               bool       if_not_exists)
{
	// make sure udf does not exists
	auto current = udf_mgr_find(self, &config->user, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("function '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create udf
	auto udf = udf_allocate(config, self->free, self->free_arg);
	rel_mgr_create(&self->mgr, tr, &udf->rel);
	return true;
}

static void
replace_if_commit(Log* self, LogOp* op)
{
	// free previous config and data by constructing
	// temprorary udf object
	UdfMgr* mgr = op->iface_arg;
	auto data = (void**)log_data_of(self, op);
	auto tmp = udf_allocate_as(data[0], data[1], mgr->free, mgr->free_arg);
	udf_free(tmp, false);
}

static void
replace_if_abort(Log* self, LogOp* op)
{
	// free current config and data by constructing
	// temprorary udf object
	UdfMgr* mgr = op->iface_arg;
	auto udf = udf_of(op->rel);

	auto tmp = udf_allocate_as(udf->config, udf->data, mgr->free, mgr->free_arg);
	udf_free(tmp, false);

	// set previous config and data
	auto data = (void**)log_data_of(self, op);
	udf->config = data[0];
	udf->data   = data[1];

	// update rel data
	rel_set_user(&udf->rel, &udf->config->user);
	rel_set_name(&udf->rel, &udf->config->name);
}

static LogIf replace_if =
{
	.commit = replace_if_commit,
	.abort  = replace_if_abort
};

void
udf_mgr_replace_validate(UdfMgr* self, Tr* tr, UdfConfig* config, Udf* udf)
{
	unused(self);

	// only owner or superuser
	check_ownership(tr, &udf->rel);

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
	log_ddl(&tr->log, &replace_if, self, &udf->rel);

	buf_write(&tr->log.data, &udf->config, sizeof(void**));
	buf_write(&tr->log.data, &udf->data, sizeof(void**));

	// swap udf data and config
	udf->config = udf_new->config;
	udf->data   = udf_new->data;
	udf_new->config = NULL;
	udf_new->data   = NULL;

	// update rel data
	rel_set_user(&udf->rel, &udf->config->user);
	rel_set_name(&udf->rel, &udf->config->name);
}

void
udf_mgr_drop_of(UdfMgr* self, Tr* tr, Udf* udf)
{
	// drop udf by object
	rel_mgr_drop(&self->mgr, tr, &udf->rel);
}

bool
udf_mgr_drop(UdfMgr* self, Tr* tr, Str* user, Str* name,
             bool    if_exists)
{
	auto udf = udf_mgr_find(self, user, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &udf->rel);

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
	uint8_t* pos = log_data_of(self, op);
	Str user;
	Str name;
	unpack_string(&pos, &user);
	unpack_string(&pos, &name);

	auto udf = udf_of(op->rel);
	udf_config_set_user(udf->config, &user);
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
               Str*    user,
               Str*    name,
               Str*    user_new,
               Str*    name_new,
               bool    if_exists)
{
	auto udf = udf_mgr_find(self, user, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &udf->rel);

	// ensure new udf does not exists
	if (udf_mgr_find(self, user_new, name_new, false))
		error("function '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update udf
	log_ddl(&tr->log, &rename_if, NULL, &udf->rel);

	// save previous name
	encode_string(&tr->log.data, &udf->config->user);
	encode_string(&tr->log.data, &udf->config->name);

	// set new name
	if (! str_compare_case(&udf->config->user, user_new))
		udf_config_set_user(udf->config, user_new);

	if (! str_compare_case(&udf->config->name, name_new))
		udf_config_set_name(udf->config, name_new);

	return true;
}

static void
grant_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
grant_if_abort(Log* self, LogOp* op)
{
	// restore grants
	uint8_t* pos = log_data_of(self, op);
	auto udf = udf_of(op->rel);
	auto grants = &udf->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
udf_mgr_grant(UdfMgr*  self,
              Tr*      tr,
              Str*     user,
              Str*     name,
              Str*     to,
              bool     grant,
              uint32_t perms,
              bool     if_exists)
{
	auto udf = udf_mgr_find(self, user, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("udf '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &udf->rel);

	// validate permissions
	auto perms_all = PERM_EXECUTE;
	perms = permission_validate(user, name, perms, perms_all);

	// update udf
	log_ddl(&tr->log, &grant_if, NULL, &udf->rel);

	// save previous grants
	auto grants = &udf->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}

void
udf_mgr_dump(UdfMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Rel, link));
		udf_config_write(udf->config, buf, 0);
	}
	encode_array_end(buf);
}

Udf*
udf_mgr_find(UdfMgr* self, Str* user, Str* name,
             bool    error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, user, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return udf_of(rel);
}

Buf*
udf_mgr_list(UdfMgr* self, Str* user, Str* name, int flags)
{
	auto buf = buf_create();
	if (user && name)
	{
		// show udf
		auto udf = udf_mgr_find(self, user, name, false);
		if (udf)
			udf_config_write(udf->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show udfs
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Rel, link));
		if (user && !str_compare_case(&udf->config->user, user))
			continue;
		udf_config_write(udf->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
