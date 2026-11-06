
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_user.h>

static void
user_limits_sync(User* self)
{
	// memory
	auto limits = &self->config->limits;
	if (limits_is_set(limits, LIMIT_MEMORY))
		usage_set_limit(&self->memory, true, limits_get(limits, LIMIT_MEMORY));
	else
		usage_set_limit(&self->memory, false, 0);
}

static inline void
user_free(User* self, bool drop)
{
	unused(drop);

	// undeploy
	self->users->iface->detach(self->users, self);

	catalog_free(&self->catalog);
	if (self->config)
		user_config_free(self->config);
	am_free(self);
}

static inline void
user_show(User* self, Buf* buf, int flags)
{
	if (flags_has(flags, FCREATE))
		describe_user(self, buf, flags);
	else
		user_config_write(self->config, buf, flags);
}

static inline User*
user_allocate(Users* users, UserConfig* config)
{
	User* self = am_malloc(sizeof(User));
	self->config      = user_config_copy(config);
	self->commit      = 0;
	self->inv_seq     = 0;
	self->inv_revoked = 0;
	self->compute     = NULL;
	self->users       = users;

	// prepare memory usage
	usage_init(&self->memory, "memory");
	user_limits_sync(self);

	// prepare catalog
	catalog_init(&self->catalog,
	             users->iface_catalog,
	             users->iface_arg,
	             &self->memory,
	             &self->config->name);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_USER);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_show(rel, (RelShow)user_show);
	rel_set_free(rel, (RelFree)user_free);
	return self;
}

static inline void
user_sync_revoke(User* self)
{
	if (str_empty(&self->config->revoked_at))
	{
		self->inv_revoked = 0;
		return;
	}
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_set(&ts, &self->config->revoked_at);
	auto time = timestamp_get_unixtime(&ts, runtime()->timezone);
	self->inv_revoked = time / 1000 / 1000;
}

bool
user_create(Users*      self,
            Tr*         tr,
            UserConfig* config,
            bool        if_not_exists)
{
	// PERM_CREATE_USER
	check_permission(tr, PERM_CREATE_USER);

	// make sure user does not exists
	auto user = users_find(self, &config->name, false);
	if (user)
	{
		if (! if_not_exists)
			error("user '{str}': already exists", &config->name);
		return false;
	}

#if 0
	// check limit
	catalog_limit(self, tr, REL_USER, LIMIT_USERS);
#endif

	// create user
	user = user_allocate(self, config);
	rels_create(&self->users, tr, &user->rel);

	// update timestamps
	user_sync_revoke(user);

	// deploy user
	self->iface->attach(self, user);
	return true;
}

static void
user_drop_of(Users* self,
             Tr*    tr,
             Rel*   user,
             bool   cascade)
{
	// check dependencies
	list_foreach_safe(&self->users.list)
	{
		auto ref = user_of(list_at(Rel, link));
		if (&ref->rel == user || !str_compare(&ref->config->parent, user->name))
			continue;
		if (cascade)
			user_drop_of(self, tr, &ref->rel, true);
		else
			error("user '{str}': has children", user->name);
	}

	// invalidate tokens
	user_invalidate(user_of(user));

	// drop user by object
	rels_drop(&self->users, tr, user);
}

bool
user_drop(Users* self,
          Tr*    tr,
          Str*   name,
          bool   if_exists,
          bool   cascade)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

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

	Users* catalog = op->iface_arg;
	rels_rename(&catalog->users, op->rel, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
user_rename(Users* self,
            Tr*    tr,
            Str*   name,
            Str*   name_new,
            bool   if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot be renamed", name);

	// ensure new user does not exists
	if (users_find(self, name_new, false))
		error("user '{str}': already exists", name_new);

	// invalidate tokens
	user_invalidate(user);

	// update user
	log_ddl(&tr->log, &rename_if, self, &user->rel);

	// save name for rollback
	encode_str(&tr->log.data, name);

	// set new name
	rels_rename(&self->users, &user->rel, name_new);
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
user_grant(Users*   self,
           Tr*      tr,
           Str*     name,
           bool     grant,
           uint32_t perms,
           bool     if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user cannot change grants", name);

#if 0
	// validate permissions
	auto perms_all =
		PERM_GRANT           |
		PERM_SYSTEM          |
		PERM_CREATE_USER     |
		PERM_CREATE_TOKEN    |
		PERM_CREATE_TABLE    |
		PERM_CREATE_FUNCTION |
		PERM_CREATE_API      |
		PERM_SQL;
	perms = permission_validate(NULL, name, perms, perms_all);
#endif

	// PERM_SYSTEM for system wide grants
	if (((perms & PERM_SYSTEM) > 0) ||
	    ((perms & PERM_CREATE_USER) > 0))
		check_permission(tr, PERM_SYSTEM);

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
	user_sync_revoke(user);
}

static LogIf revoke_token_if =
{
	.commit = revoke_token_if_commit,
	.abort  = revoke_token_if_abort
};

bool
user_revoke_token(Users* self,
                  Tr*    tr,
                  Str*   name,
                  Str*   revoked_at,
                  bool   if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user is immutable", name);

	// invalidate tokens
	user_invalidate(user);

	// update user
	log_ddl(&tr->log, &revoke_token_if, NULL, &user->rel);

	// save previous revoked_at for rollback
	encode_str(&tr->log.data, &user->config->revoked_at);

	// set revoked_at
	user_config_set_revoked_at(user->config, revoked_at);

	// update timestamps
	user_sync_revoke(user);
	return true;
}

static void
describe_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
describe_if_abort(Log* self, LogOp* op)
{
	// set previous description
	uint8_t* pos = log_data_of(self, op);
	Str value;
	unpack_str(&pos, &value);

	auto user = user_of(op->rel);
	user_config_set_description(user->config, &value);
}

static LogIf describe_if =
{
	.commit = describe_if_commit,
	.abort  = describe_if_abort
};

bool
user_describe(Users* self,
              Tr*    tr,
              Str*   name,
              Str*   description,
              bool   if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// superuser is immutable
	if (user->config->superuser)
		error("user '{str}': system user is immutable", name);

	// update user
	log_ddl(&tr->log, &describe_if, NULL, &user->rel);

	// save previous description
	encode_str(&tr->log.data, &user->config->description);

	// set new description
	user_config_set_description(user->config, description);
	return true;
}

static void
limit_set_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
limit_set_if_abort(Log* self, LogOp* op)
{
	// set previous limits
	uint8_t* pos = log_data_of(self, op);
	Limits limits;
	limits_init(&limits);
	limits_read(&limits, &pos);

	auto user = user_of(op->rel);
	limits_copy(&user->config->limits, &limits);

	user_limits_sync(user);
}

static LogIf limit_set_if =
{
	.commit = limit_set_if_commit,
	.abort  = limit_set_if_abort
};

bool
user_limit_set(Users*  self,
               Tr*     tr,
               Str*    name,
               Limits* limits,
               bool    if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// update user
	log_ddl(&tr->log, &limit_set_if, NULL, &user->rel);

	// save previous limits
	limits_write(&user->config->limits, &tr->log.data);

	// set new limits
	limits_copy(&user->config->limits, limits);

	user_limits_sync(user);
	return true;
}

bool
user_limit_unset(Users*   self,
                 Tr*      tr,
                 Str*     name,
                 uint64_t mask,
                 bool     if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// update user
	log_ddl(&tr->log, &limit_set_if, NULL, &user->rel);

	// save previous limits
	limits_write(&user->config->limits, &tr->log.data);

	// update limits
	user->config->limits.flags &= ~mask;

	user_limits_sync(user);
	return true;
}

static void
api_set_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
api_set_if_abort(Log* self, LogOp* op)
{
	// restore previous apis
	auto user = user_of(op->rel);
	apis_free(&user->config->apis);

	uint8_t* pos = log_data_of(self, op);
	apis_read(&user->config->apis, &pos);
}

static LogIf api_set_if =
{
	.commit = api_set_if_commit,
	.abort  = api_set_if_abort
};

bool
user_api_create(Users* self,
                Tr*    tr,
                Str*   name,
                Api*   api,
                bool   if_exists)
{
	// PERM_CREATE_API
	check_permission(tr, PERM_CREATE_API);

	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// ensure api is not redefined
	auto ref = apis_find(&user->config->apis, &api->uri);
	if (ref)
		error("user '{str}': api '{str}' already exists",
		      name, &api->uri);

	// update user
	log_ddl(&tr->log, &api_set_if, NULL, &user->rel);

	// save previous apis
	apis_write(&user->config->apis, &tr->log.data, 0);

	// add new api
	auto copy = api_copy(api);
	apis_add(&user->config->apis, copy);
	return true;
}

bool
user_api_drop(Users* self,
              Tr*    tr,
              Str*   name,
              Str*   uri,
              bool   if_exists)
{
	auto user = users_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	user_check_ownership(tr, user);

	// ensure api is not redefined
	auto ref = apis_find(&user->config->apis, uri);
	if (! ref)
		error("user '{str}': api '{str}' not exists",
		      name, uri);

	// update user
	log_ddl(&tr->log, &api_set_if, NULL, &user->rel);

	// save previous apis
	apis_write(&user->config->apis, &tr->log.data, 0);

	// remove api
	apis_remove(&user->config->apis, ref);
	api_free(ref);
	return true;
}
