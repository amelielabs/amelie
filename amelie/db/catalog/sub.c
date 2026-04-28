
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

static void
sub_free(Sub* self, bool drop)
{
	unused(drop);
	catalog_cdc_unref(self->catalog, &self->on_id);
	cdc_detach(self->catalog->cdc, &self->slot);
	sub_config_free(self->config);
	am_free(self);
}

static inline void
sub_show(Sub* self, Buf* buf, int flags)
{
	sub_config_write(self->config, buf, flags);
}

static Sub*
sub_allocate(SubConfig* config, Catalog* catalog, Uuid* id)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->config  = sub_config_copy(config);
	self->catalog = catalog;
	self->on_id   = *id;
	cdc_slot_init(&self->slot);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_SUBSCRIPTION);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_id(rel, &self->config->id);
	rel_set_show(rel, (RelShow)sub_show);
	rel_set_free(rel, (RelFree)sub_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
sub_create(Catalog* self, Tr* tr, SubConfig* config, bool if_not_exists)
{
	// PERM_CREATE_SUB
	check_user(tr, PERM_CREATE_SUB);

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
sub_drop_of(Catalog* self, Tr* tr, Sub* sub)
{
	// drop sub by object
	rel_mgr_drop(&self->rels, tr, &sub->rel);
}

bool
sub_drop(Catalog* self, Tr* tr, Str* user, Str* name, bool if_exists)
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

	sub_drop_of(self, tr, sub);
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

	Catalog* catalog = op->iface_arg;
	rel_mgr_rename(&catalog->rels, op->rel, &user, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
sub_rename(Catalog* self,
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
	rel_mgr_rename(&self->rels, &sub->rel, user_new, name_new);
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
sub_grant(Catalog* self,
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
