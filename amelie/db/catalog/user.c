
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
user_free(User* self, bool drop)
{
	unused(drop);
	if (self->config)
		user_config_free(self->config);
	am_free(self);
}

static inline void
user_show(User* self, Buf* buf, int flags)
{
	user_config_write(self->config, buf, flags);
}

static inline User*
user_allocate(UserConfig* config)
{
	User* self = am_malloc(sizeof(User));
	self->revoked_at = 0;
	self->config     = user_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_USER);
	rel_set_user(rel, &self->config->parent);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)user_show);
	rel_set_free(rel, (RelFree)user_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
user_create(Catalog*    self,
            Tr*         tr,
            UserConfig* config,
            bool        if_not_exists)
{
	// PERM_CREATE_USER
	//
	// (skip user check on bootstrap)
	if (tr->user)
		check_user(tr, PERM_CREATE_USER);

	// make sure user does not exists
	auto user = catalog_find_user(self, &config->name, false);
	if (user)
	{
		if (! if_not_exists)
			error("user '{str}': already exists", &config->name);
		return false;
	}

	// create user
	user = user_allocate(config);
	rel_mgr_create(&self->users, tr, &user->rel);

	// update timestamps
	user_sync(user);
	return true;
}

bool
user_drop(Catalog* self,
          Tr*      tr,
          Str*     name,
          bool     if_exists)
{
	auto user = catalog_find_user(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// main user is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot be dropped", name);

	// drop user by object
	rel_mgr_drop(&self->users, tr, &user->rel);
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
	// set previous name
	uint8_t* pos = log_data_of(self, op);
	Str name;
	unpack_str(&pos, &name);

	Catalog* catalog = op->iface_arg;
	rel_mgr_rename(&catalog->users, op->rel, NULL, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
user_rename(Catalog* self,
            Tr*      tr,
            Str*     name,
            Str*     name_new,
            bool     if_exists)
{
	auto user = catalog_find_user(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// main user is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot be renamed", name);

	// ensure new user does not exists
	if (catalog_find_user(self, name_new, false))
		error("user '{str}': already exists", name_new);

	// update user
	log_ddl(&tr->log, &rename_if, self, &user->rel);

	// save name for rollback
	encode_str(&tr->log.data, name);

	// set new name
	rel_mgr_rename(&self->users, &user->rel, NULL, name_new);
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
	auto user = user_of(op->rel);
	auto grants = &user->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
user_grant(Catalog* self,
           Tr*      tr,
           Str*     name,
           bool     grant,
           uint32_t perms,
           bool     if_exists)
{
	auto user = catalog_find_user(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// main user is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot change grants", name);

	// validate permissions
	auto perms_all =
	     PERM_GRANT           |
	     PERM_SYSTEM          |
	     PERM_CREATE_USER     |
	     PERM_CREATE_TOKEN    |
	     PERM_CREATE_TABLE    |
	     PERM_CREATE_FUNCTION |
	     PERM_CREATE_TOPIC    |
	     PERM_CREATE_SUB      |
	     PERM_CONNECT         |
	     PERM_SERVICE;
	perms = permission_validate(NULL, name, perms, perms_all);

	// update user
	log_ddl(&tr->log, &grant_if, NULL, &user->rel);

	// save previous grants
	auto grants = &user->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	Str to;
	str_set(&to, "self", 4);
	if (grant)
		grants_add(grants, &to, perms);
	else
		grants_remove(grants, &to, perms);
	return true;
}

static void
revoke_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
revoke_if_abort(Log* self, LogOp* op)
{
	// set previous revoked_at
	uint8_t* pos = log_data_of(self, op);
	Str value;
	unpack_str(&pos, &value);

	auto user = user_of(op->rel);
	user_config_set_revoked_at(user->config, &value);
	user_sync(user);
}

static LogIf revoke_if =
{
	.commit = revoke_if_commit,
	.abort  = revoke_if_abort
};

bool
user_revoke(Catalog* self,
            Tr*      tr,
            Str*     name,
            Str*     revoked_at,
            bool     if_exists)
{
	auto user = catalog_find_user(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// invalidate auth caches
	self->iface->user_invalidate(self, user);

	// update user
	log_ddl(&tr->log, &revoke_if, NULL, &user->rel);

	// save previous revoked_at for rollback
	encode_str(&tr->log.data, &user->config->revoked_at);

	// set revoked_at
	user_config_set_revoked_at(user->config, revoked_at);

	// update timestamps
	user_sync(user);
	return true;
}
