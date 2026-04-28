
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
topic_free(Topic* self, bool drop)
{
	unused(drop);
	topic_config_free(self->config);
	am_free(self);
}

static inline void
topic_show(Topic* self, Buf* buf, int flags)
{
	topic_config_write(self->config, buf, flags);
}

static inline Topic*
topic_allocate(TopicConfig* config)
{
	auto self = (Topic*)am_malloc(sizeof(Topic));
	self->cdc    = 0;
	self->config = topic_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_TOPIC);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_id(rel, &self->config->id);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)topic_show);
	rel_set_free(rel, (RelFree)topic_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
topic_create(Catalog*     self,
             Tr*          tr,
             TopicConfig* config,
             bool         if_not_exists)
{
	// PERM_CREATE_TOPIC
	check_user(tr, PERM_CREATE_TOPIC);

	// make sure topic does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create topic
	auto topic = topic_allocate(config);
	rel_mgr_create(&self->rels, tr, &topic->rel);
	return true;
}

void
topic_drop_of(Catalog* self, Tr* tr, Topic* topic)
{
	// drop topic by object
	rel_mgr_drop(&self->rels, tr, &topic->rel);
}

bool
topic_drop(Catalog* self, Tr* tr, Str* user, Str* name,
           bool     if_exists)
{
	auto topic = catalog_find_topic(self, user, name, false);
	if (! topic)
	{
		if (! if_exists)
			error("topic '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &topic->rel);

	topic_drop_of(self, tr, topic);
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
topic_rename(Catalog* self,
             Tr*      tr,
             Str*     user,
             Str*     name,
             Str*     user_new,
             Str*     name_new,
             bool     if_exists)
{
	auto topic = catalog_find_topic(self, user, name, false);
	if (! topic)
	{
		if (! if_exists)
			error("topic '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &topic->rel);

	// ensure other relation with the same name does not exists
	if (catalog_find(self, REL_UNDEF, user_new, name_new, false))
		error("relation '%.*s': already exists", str_size(name_new),
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
topic_grant(Catalog* self,
            Tr*      tr,
            Str*     user,
            Str*     name,
            Str*     to,
            bool     grant,
            uint32_t perms,
            bool     if_exists)
{
	auto topic = catalog_find_topic(self, user, name, false);
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
