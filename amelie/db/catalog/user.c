
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

static void
user_drop_of(Catalog* self,
             Tr*      tr,
             Rel*     user,
             bool     cascade)
{
	if (cascade)
	{
		// drop cascade all owned relations
		Buf deps;
		buf_init(&deps);
		defer_buf(&deps);

		auto count = 0;
		list_foreach_safe(&self->rels.list)
		{
			auto rel = list_at(Rel, link);
			if (! str_compare(rel->user, user->name))
			{
				// revoke all permissions
				if (grants_find(rel->grants, user->name))
					catalog_grant_of(self, tr, rel, user->name, false, PERM_ALL);
				continue;
			}
			count += catalog_deps(self, rel, &deps) + 1;
			catalog_deps_add(&deps, rel);
		}

		// drop relations
		if (count > 0)
			catalog_deps_drop(self, tr, &deps);

		// drop cascade all child users
		list_foreach_safe(&self->users.list)
		{
			auto rel = list_at(Rel, link);
			if (rel == user || !str_compare(rel->user, user->name))
				continue;
			user_drop_of(self, tr, rel, true);
		}

	} else
	{
		// ensure user has no relations
		list_foreach(&self->rels.list)
		{
			auto rel = list_at(Rel, link);
			if (str_compare(rel->user, user->name))
				error("user '{str}' still has {s} '{str}'", user->name,
				      rel_type_of(rel->type), rel->name);

			// revoke all permissions
			if (grants_find(rel->grants, user->name))
				catalog_grant_of(self, tr, rel, user->name, false, PERM_ALL);
		}
	}

	// invalidate auth caches
	self->iface->user_invalidate(self, user_of(user));

	// drop user by object
	rel_mgr_drop(&self->users, tr, user);
}

bool
user_drop(Catalog* self,
          Tr*      tr,
          Str*     name,
          bool     if_exists,
          bool     cascade)
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

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot be dropped", name);

	// drop user relations, user and child users
	user_drop_of(self, tr, &user->rel, cascade);
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

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot be renamed", name);

	// ensure new user does not exists
	if (catalog_find_user(self, name_new, false))
		error("user '{str}': already exists", name_new);

	// ensure no strict dependecies on the user name (udfs, clones or users)
	catalog_deps_validate_user(self, name, true);

	// rename own relations and grants
	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (str_compare(rel->user, name))
			catalog_rename_of(self, tr, rel, name_new, rel->name);

		// rename grants
		if (grants_find(rel->grants, name))
			catalog_grant_rename_of(self, tr, rel, name, name_new);
	}

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

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot change grants", name);

	// validate permissions
	auto perms_all =
	     PERM_GRANT               |
	     PERM_SYSTEM              |
	     PERM_CREATE_USER         |
	     PERM_CREATE_TOKEN        |
	     PERM_CREATE_TABLE        |
	     PERM_CREATE_CLONE        |
	     PERM_CREATE_FUNCTION     |
	     PERM_CREATE_TOPIC        |
	     PERM_CREATE_SUBSCRIPTION |
	     PERM_RPC                 |
	     PERM_SQL                 |
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
revoke_token_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
revoke_token_if_abort(Log* self, LogOp* op)
{
	// set previous revoked_at
	uint8_t* pos = log_data_of(self, op);
	Str value;
	unpack_str(&pos, &value);

	auto user = user_of(op->rel);
	user_config_set_revoked_at(user->config, &value);
	user_sync(user);
}

static LogIf revoke_token_if =
{
	.commit = revoke_token_if_commit,
	.abort  = revoke_token_if_abort
};

bool
user_revoke_token(Catalog* self,
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
	log_ddl(&tr->log, &revoke_token_if, NULL, &user->rel);

	// save previous revoked_at for rollback
	encode_str(&tr->log.data, &user->config->revoked_at);

	// set revoked_at
	user_config_set_revoked_at(user->config, revoked_at);

	// update timestamps
	user_sync(user);
	return true;
}
