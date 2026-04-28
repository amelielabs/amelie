
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

bool
sub_mgr_create(Catalog* self, Tr* tr, SubConfig* config, bool if_not_exists)
{
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// find and use relation for cdc
	Uuid* id;
	catalog_cdc_ref(self, user_of(tr->user),
	                &config->on_user,
	                &config->on,
	                &id);

	// allocate storage
	auto sub = sub_allocate(config, self, id);

	// register storage
	rel_mgr_create(&self->rels, tr, &sub->rel);

	// set pos and prepare slot
	cdc_slot_set(&sub->slot, config->lsn, config->op);

	// attach slot
	cdc_attach(self->cdc, &sub->slot);
	return true;
}

void
sub_mgr_drop_of(Catalog* self, Tr* tr, Sub* sub)
{
	// drop sub by object
	rel_mgr_drop(&self->rels, tr, &sub->rel);
}

bool
sub_mgr_drop(Catalog* self, Tr* tr, Str* user, Str* name, bool if_exists)
{
	auto sub = catalog_find_sub(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &sub->rel);

	sub_mgr_drop_of(self, tr, sub);
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

	auto sub = sub_of(op->rel);
	sub_config_set_user(sub->config, &user);
	sub_config_set_name(sub->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
sub_mgr_rename(Catalog* self,
               Tr*      tr,
               Str*     user,
               Str*     name,
               Str*     user_new,
               Str*     name_new,
               bool     if_exists)
{
	auto sub = catalog_find_sub(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &sub->rel);

	// ensure other relation with the same name does not exists
	if (catalog_find(self, REL_UNDEF, user_new, name_new, false))
		error("relation '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update sub
	log_ddl(&tr->log, &rename_if, NULL, &sub->rel);

	// save previous name
	encode_str(&tr->log.data, &sub->config->user);
	encode_str(&tr->log.data, &sub->config->name);

	// set new name
	if (! str_compare_case(&sub->config->user, user_new))
		sub_config_set_user(sub->config, user_new);

	if (! str_compare_case(&sub->config->name, name_new))
		sub_config_set_name(sub->config, name_new);

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
	auto sub = sub_of(op->rel);
	auto grants = &sub->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
sub_mgr_grant(Catalog* self,
              Tr*      tr,
              Str*     user,
              Str*     name,
              Str*     to,
              bool     grant,
              uint32_t perms,
              bool     if_exists)
{
	auto sub = catalog_find_sub(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &sub->rel);

	// validate permissions
	auto perms_all = PERM_NONE;
	perms = permission_validate(user, name, perms, perms_all);

	// update sub
	log_ddl(&tr->log, &grant_if, NULL, &sub->rel);

	// save previous grants
	auto grants = &sub->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}
