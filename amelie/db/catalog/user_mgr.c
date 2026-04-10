
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
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
user_mgr_init(UserMgr* self)
{
	rel_mgr_init(&self->mgr);
}

void
user_mgr_free(UserMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
user_mgr_create(UserMgr*    self,
                Tr*         tr,
                UserConfig* config,
                bool        if_not_exists)
{
	// make sure user does not exists
	auto current = user_mgr_find(self, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("user '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create user
	auto user = user_allocate(config);
	rel_mgr_create(&self->mgr, tr, &user->rel);

	// update timestamps
	user_sync(user);
	return true;
}

bool
user_mgr_drop(UserMgr* self,
              Tr*      tr,
              Str*     name,
              bool     if_exists)
{
	auto user = user_mgr_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// main user is immutable
	if (user->config->superuser)
		error("user '%.*s': system user cannot be dropped", str_size(name),
		      str_of(name));

	// drop user by object
	rel_mgr_drop(&self->mgr, tr, &user->rel);
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
	json_read_string(&pos, &name);

	auto user = user_of(op->rel);
	user_config_set_name(user->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
user_mgr_rename(UserMgr* self,
                Tr*      tr,
                Str*     name,
                Str*     name_new,
                bool     if_exists)
{
	auto user = user_mgr_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// main user is immutable
	if (user->config->superuser)
		error("user '%.*s': system user cannot be renamed", str_size(name),
		       str_of(name));

	// ensure new user does not exists
	if (user_mgr_find(self, name_new, false))
		error("user '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update user
	log_rel(&tr->log, &rename_if, NULL, &user->rel);

	// save name for rollback
	encode_string(&tr->log.data, name);

	// set new name
	user_config_set_name(user->config, name_new);
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
user_mgr_grant(UserMgr* self,
               Tr*      tr,
               Str*     name,
               bool     grant,
               uint32_t perms,
               bool     if_exists)
{
	auto user = user_mgr_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// main user is immutable
	if (user->config->superuser)
		error("user '%.*s': system user cannot change grants", str_size(name),
		       str_of(name));

	// validate permissions
	auto perms_all =
	     PERM_GRANT           |
	     PERM_SYSTEM          |
	     PERM_CREATE_USER     |
	     PERM_CREATE_TOKEN    |
	     PERM_CREATE_TABLE    |
	     PERM_CREATE_FUNCTION |
	     PERM_CREATE_CHANNEL  |
	     PERM_CREATE_SUB      |
	     PERM_CONNECT         |
	     PERM_SERVICE;
	perms = permission_validate(NULL, name, perms, perms_all);

	// update user
	log_rel(&tr->log, &grant_if, NULL, &user->rel);

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
	json_read_string(&pos, &value);

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
user_mgr_revoke(UserMgr* self,
                Tr*      tr,
                Str*     name,
                Str*     revoked_at,
                bool     if_exists)
{
	auto user = user_mgr_find(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership_user(tr, &user->rel);

	// update user
	log_rel(&tr->log, &revoke_if, NULL, &user->rel);

	// save previous revoked_at for rollback
	encode_string(&tr->log.data, &user->config->revoked_at);

	// set revoked_at
	user_config_set_revoked_at(user->config, revoked_at);

	// update timestamps
	user_sync(user);
	return true;
}

void
user_mgr_dump(UserMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto user = user_of(list_at(Rel, link));
		user_config_write(user->config, buf, FSECRETS);
	}
	encode_array_end(buf);
}

User*
user_mgr_find(UserMgr* self, Str* name, bool error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, NULL, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return user_of(rel);
}

Buf*
user_mgr_list(UserMgr* self, Str* name, bool agents_only, int flags)
{
	auto buf = buf_create();
	if (name)
	{
		// show user
		auto user = user_mgr_find(self, name, false);
		if (user)
			user_config_write(user->config, buf, flags);
		else
			encode_null(buf);
	} else
	{
		// show users/agents
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto user = user_of(list_at(Rel, link));
			if (agents_only && !user->config->agent)
				continue;
			user_config_write(user->config, buf, flags);
		}
		encode_array_end(buf);
	}
	return buf;
}
