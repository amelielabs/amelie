
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

static inline void
udf_free(Udf* self, bool drop)
{
	unused(drop);
	if (self->free)
		self->free(self);
	if (self->config)
		udf_config_free(self->config);
	am_free(self);
}

static inline void
udf_show(Udf* self, Buf* buf, int flags)
{
	udf_config_write(self->config, buf, flags);
}

static inline Udf*
udf_allocate_as(UdfConfig* config, void* data, UdfFree free, void* free_arg)
{
	auto self = (Udf*)am_malloc(sizeof(Udf));
	self->free     = free;
	self->free_arg = free_arg;
	self->data     = data;
	self->config   = config;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_UDF);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)udf_show);
	rel_set_free(rel, (RelFree)udf_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Udf*
udf_allocate(UdfConfig* config, UdfFree free, void* free_arg)
{
	return udf_allocate_as(udf_config_copy(config), NULL, free, free_arg);
}

static void
replace_if_commit(Log* self, LogOp* op)
{
	// free previous config and data by constructing
	// temprorary udf object
	Catalog* catalog = op->iface_arg;
	auto data = (void**)log_data_of(self, op);
	auto tmp = udf_allocate_as(data[0], data[1], catalog->iface->udf_free,
	                           catalog->iface_arg);
	udf_free(tmp, false);
}

static void
replace_if_abort(Log* self, LogOp* op)
{
	// free current config and data by constructing
	// temprorary udf object
	Catalog* catalog = op->iface_arg;
	auto udf = udf_of(op->rel);
	auto tmp = udf_allocate_as(udf->config, udf->data, catalog->iface->udf_free,
	                           catalog->iface_arg);
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

static void
udf_replace(Catalog*   self,
            Tr*        tr,
            Udf*       udf,
            UdfConfig* config)
{
	// only owner or superuser
	check_ownership(tr, &udf->rel);

	// allow to replace udf only if it has identical arguments
	// and return type

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

	// create and compile new temporary udf
	auto udf_new = udf_allocate(config, self->iface->udf_free, self->iface_arg);
	defer(rel_free, udf_new);

	// compile on creation
	self->iface->udf_compile(self, udf_new);

	assert(udf->data);
	assert(udf_new->data);

	// update log
	log_ddl(&tr->log, &replace_if, self, &udf->rel);

	// save previous udf config and data
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

bool
udf_create(Catalog*   self,
           Tr*        tr,
           UdfConfig* config,
           bool       or_replace)
{
	// PERM_CREATE_FUNCTION
	check_user(tr, PERM_CREATE_FUNCTION);

	// find existing udf
	auto udf = catalog_find_udf(self, &config->user, &config->name, false);
	if (udf)
	{
		// replace
		if (! or_replace)
			error("function '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));

		udf_replace(self, tr, udf, config);
		return true;
	}

	// make sure relation does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
		error("relation '%.*s': already exists", str_size(&config->name),
		      str_of(&config->name));

	// create udf
	udf = udf_allocate(config, self->iface->udf_free, self->iface_arg);
	rel_mgr_create(&self->rels, tr, &udf->rel);

	// compile on creation
	self->iface->udf_compile(self, udf);
	return true;
}

void
udf_drop_of(Catalog* self, Tr* tr, Udf* udf)
{
	// drop udf by object
	rel_mgr_drop(&self->rels, tr, &udf->rel);
}

bool
udf_drop(Catalog* self, Tr* tr, Str* user, Str* name,
         bool     if_exists)
{
	auto udf = catalog_find_udf(self, user, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &udf->rel);

	udf_drop_of(self, tr, udf);
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
	unpack_str(&pos, &user);
	unpack_str(&pos, &name);

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
udf_rename(Catalog* self,
           Tr*      tr,
           Str*     user,
           Str*     name,
           Str*     user_new,
           Str*     name_new,
           bool     if_exists)
{
	auto udf = catalog_find_udf(self, user, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &udf->rel);

	// ensure other relation with the same name does not exists
	if (catalog_find(self, REL_UNDEF, user_new, name_new, false))
		error("relation '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update udf
	log_ddl(&tr->log, &rename_if, NULL, &udf->rel);

	// save previous name
	encode_str(&tr->log.data, &udf->config->user);
	encode_str(&tr->log.data, &udf->config->name);

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
udf_grant(Catalog* self,
          Tr*      tr,
          Str*     user,
          Str*     name,
          Str*     to,
          bool     grant,
          uint32_t perms,
          bool     if_exists)
{
	auto udf = catalog_find_udf(self, user, name, false);
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
