
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
topic_mgr_init(TopicMgr* self)
{
	rel_mgr_init(&self->mgr);
}

void
topic_mgr_free(TopicMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
topic_mgr_create(TopicMgr*    self,
                 Tr*          tr,
                 TopicConfig* config,
                 bool         if_not_exists)
{
	// make sure topic does not exists
	auto topic = topic_mgr_find(self, &config->user, &config->name, false);
	if (topic)
	{
		if (! if_not_exists)
			error("topic '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create topic
	topic = topic_allocate(config);
	rel_mgr_create(&self->mgr, tr, &topic->rel);
	return true;
}

void
topic_mgr_drop_of(TopicMgr* self, Tr* tr, Topic* topic)
{
	// drop topic by object
	rel_mgr_drop(&self->mgr, tr, &topic->rel);
}

bool
topic_mgr_drop(TopicMgr* self, Tr* tr, Str* user, Str* name,
               bool      if_exists)
{
	auto topic = topic_mgr_find(self, user, name, false);
	if (! topic)
	{
		if (! if_exists)
			error("topic '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &topic->rel);

	topic_mgr_drop_of(self, tr, topic);
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

	auto topic = topic_of(op->rel);
	topic_config_set_user(topic->config, &user);
	topic_config_set_name(topic->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
topic_mgr_rename(TopicMgr* self,
                 Tr*       tr,
                 Str*      user,
                 Str*      name,
                 Str*      user_new,
                 Str*      name_new,
                 bool      if_exists)
{
	auto topic = topic_mgr_find(self, user, name, false);
	if (! topic)
	{
		if (! if_exists)
			error("topic '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &topic->rel);

	// ensure new topic does not exists
	if (topic_mgr_find(self, user_new, name_new, false))
		error("topic '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update topic
	log_ddl(&tr->log, &rename_if, NULL, &topic->rel);

	// save previous name
	encode_str(&tr->log.data, &topic->config->user);
	encode_str(&tr->log.data, &topic->config->name);

	// set new name
	if (! str_compare_case(&topic->config->user, user_new))
		topic_config_set_user(topic->config, user_new);

	if (! str_compare_case(&topic->config->name, name_new))
		topic_config_set_name(topic->config, name_new);

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
	auto topic = topic_of(op->rel);
	auto grants = &topic->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
topic_mgr_grant(TopicMgr* self,
                Tr*       tr,
                Str*      user,
                Str*      name,
                Str*      to,
                bool      grant,
                uint32_t  perms,
                bool      if_exists)
{
	auto topic = topic_mgr_find(self, user, name, false);
	if (! topic)
	{
		if (! if_exists)
			error("topic '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &topic->rel);

	// validate permissions
	auto perms_all = PERM_PUBLISH;
	perms = permission_validate(user, name, perms, perms_all);

	// update topic
	log_ddl(&tr->log, &grant_if, NULL, &topic->rel);

	// save previous grants
	auto grants = &topic->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}

void
topic_mgr_dump(TopicMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto topic = topic_of(list_at(Rel, link));
		topic_config_write(topic->config, buf, 0);
	}
	encode_array_end(buf);
}

Topic*
topic_mgr_find(TopicMgr* self, Str* user, Str* name,
               bool      error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, user, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("topic '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return topic_of(rel);
}

void
topic_mgr_list(TopicMgr* self, Buf* buf, Str* user, Str* name, int flags)
{
	if (user && name)
	{
		// show topic
		auto topic = topic_mgr_find(self, user, name, false);
		if (topic)
			topic_config_write(topic->config, buf, flags);
		else
			encode_null(buf);
		return;
	}

	// show topics
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto topic = topic_of(list_at(Rel, link));
		if (user && !str_compare_case(&topic->config->user, user))
			continue;
		topic_config_write(topic->config, buf, flags);
	}
	encode_array_end(buf);
}

Topic*
topic_mgr_find_by(TopicMgr* self, Uuid* id, bool error_if_not_exists)
{
	list_foreach(&self->mgr.list)
	{
		auto topic = topic_of(list_at(Rel, link));
		if (uuid_is(&topic->config->id, id))
			return topic;
	}
	if (error_if_not_exists)
	{
		char uuid[UUID_SZ];
		uuid_get(id, uuid, sizeof(uuid));
		error("topic with uuid '%s' not found", uuid);
	}
	return NULL;
}
