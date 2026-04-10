
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
channel_mgr_init(ChannelMgr* self)
{
	rel_mgr_init(&self->mgr);
}

void
channel_mgr_free(ChannelMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
channel_mgr_create(ChannelMgr*    self,
                   Tr*            tr,
                   ChannelConfig* config,
                   bool           if_not_exists)
{
	// make sure channel does not exists
	auto channel = channel_mgr_find(self, &config->user, &config->name, false);
	if (channel)
	{
		if (! if_not_exists)
			error("channel '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create channel
	channel = channel_allocate(config);
	rel_mgr_create(&self->mgr, tr, &channel->rel);
	return true;
}

void
channel_mgr_drop_of(ChannelMgr* self, Tr* tr, Channel* channel)
{
	// drop channel by object
	rel_mgr_drop(&self->mgr, tr, &channel->rel);
}

bool
channel_mgr_drop(ChannelMgr* self, Tr* tr, Str* user, Str* name,
                 bool        if_exists)
{
	auto channel = channel_mgr_find(self, user, name, false);
	if (! channel)
	{
		if (! if_exists)
			error("channel '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &channel->rel);

	channel_mgr_drop_of(self, tr, channel);
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
	json_read_string(&pos, &user);
	json_read_string(&pos, &name);

	auto channel = channel_of(op->rel);
	channel_config_set_user(channel->config, &user);
	channel_config_set_name(channel->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
channel_mgr_rename(ChannelMgr* self,
                   Tr*         tr,
                   Str*        user,
                   Str*        name,
                   Str*        user_new,
                   Str*        name_new,
                   bool        if_exists)
{
	auto channel = channel_mgr_find(self, user, name, false);
	if (! channel)
	{
		if (! if_exists)
			error("channel '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &channel->rel);

	// ensure new channel does not exists
	if (channel_mgr_find(self, user_new, name_new, false))
		error("channel '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update channel
	log_rel(&tr->log, &rename_if, NULL, &channel->rel);

	// save previous name
	encode_string(&tr->log.data, &channel->config->user);
	encode_string(&tr->log.data, &channel->config->name);

	// set new name
	if (! str_compare_case(&channel->config->user, user_new))
		channel_config_set_user(channel->config, user_new);

	if (! str_compare_case(&channel->config->name, name_new))
		channel_config_set_name(channel->config, name_new);

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
	auto channel = channel_of(op->rel);
	auto grants = &channel->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
channel_mgr_grant(ChannelMgr* self,
                  Tr*         tr,
                  Str*        user,
                  Str*        name,
                  Str*        to,
                  bool        grant,
                  uint32_t    perms,
                  bool        if_exists)
{
	auto channel = channel_mgr_find(self, user, name, false);
	if (! channel)
	{
		if (! if_exists)
			error("channel '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &channel->rel);

	// validate permissions
	auto perms_all = PERM_PUBLISH;
	perms = permission_validate(user, name, perms, perms_all);

	// update channel
	log_rel(&tr->log, &grant_if, NULL, &channel->rel);

	// save previous grants
	auto grants = &channel->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}

void
channel_mgr_dump(ChannelMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto channel = channel_of(list_at(Rel, link));
		channel_config_write(channel->config, buf, 0);
	}
	encode_array_end(buf);
}

Channel*
channel_mgr_find(ChannelMgr* self, Str* user, Str* name,
                 bool        error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, user, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("channel '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return channel_of(rel);
}

Buf*
channel_mgr_list(ChannelMgr* self, Str* user, Str* name, int flags)
{
	auto buf = buf_create();
	if (user && name)
	{
		// show channel
		auto channel = channel_mgr_find(self, user, name, false);
		if (channel)
			channel_config_write(channel->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show channels
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto channel = channel_of(list_at(Rel, link));
		if (user && !str_compare_case(&channel->config->user, user))
			continue;
		channel_config_write(channel->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
