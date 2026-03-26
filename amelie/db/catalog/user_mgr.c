
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
	if (user->config->system)
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

	auto mgr = user_of(op->rel);
	user_config_set_name(mgr->config, &name);
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

	if (user->config->system)
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
