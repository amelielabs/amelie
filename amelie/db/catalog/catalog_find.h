#pragma once

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

#if 0
hot Rel*
catalog_find(Catalog* self, RelType type, Str* user, Str* name, bool error_if_not_exists)
{

	// type


	auto table = table_mgr_find(&self->table_mgr, user, name, false);
	if (table)
		return &table->rel;

	auto branch = branch_mgr_find(&self->branch_mgr, user, name, false);
	if (branch)
		return &branch->rel;

	auto udf = udf_mgr_find(&self->udf_mgr, user, name, false);
	if (udf)
		return &udf->rel;

	auto topic = topic_mgr_find(&self->topic_mgr, user, name, false);
	if (topic)
		return &topic->rel;

	auto sub = sub_mgr_find(&self->sub_mgr, user, name, false);
	if (sub)
		return &sub->rel;

	if (error_if_not_exists)
		error("relation '%.*s': not exists", str_size(name),
		      str_of(name));

	return NULL;
}

Rel*
catalog_find_by(Catalog* self, Uuid* id, bool error_if_not_exists)
{
	auto table = table_mgr_find_by(&self->table_mgr, id, false);
	if (table)
		return &table->rel;

	auto topic = topic_mgr_find_by(&self->topic_mgr, id, false);
	if (table)
		return &topic->rel;

	if (error_if_not_exists)
	{
		char uuid[UUID_SZ];
		uuid_get(id, uuid, sizeof(uuid));
		error("relation with uuid '%s' not found", uuid);
	}
	return NULL;
}

User*
catalog_find_user(Catalog* self, Str* name, bool error_if_not_exists)
{
	auto rel = rel_mgr_find(&self->users, NULL, name, error_if_not_exists);
	return user_of(rel);
}
#endif

static inline User*
catalog_find_user(Catalog* self, Str* name, bool error_if_not_exists)
{
	return user_of(rel_mgr_find(&self->users, REL_USER, NULL, name,
	                            error_if_not_exists));
}

static inline Rel*
catalog_find(Catalog* self, RelType type, Str* user, Str* name, bool error_if_not_exists)
{
	return rel_mgr_find(&self->rels, type, user, name,
	                    error_if_not_exists);
}

static inline Rel*
catalog_find_by(Catalog* self, RelType type, Uuid* id, bool error_if_not_exists)
{
	return rel_mgr_find_by(&self->rels, type, id, error_if_not_exists);
}

static inline Table*
catalog_find_table(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return table_of(rel_mgr_find(&self->rels, REL_TABLE, user, name,
	                             error_if_not_exists));
}

static inline Branch*
catalog_find_branch(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return branch_of(rel_mgr_find(&self->rels, REL_BRANCH, user, name,
	                              error_if_not_exists));
}

static inline Udf*
catalog_find_udf(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return udf_of(rel_mgr_find(&self->rels, REL_UDF, user, name,
	                           error_if_not_exists));
}

static inline Topic*
catalog_find_topic(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return topic_of(rel_mgr_find(&self->rels, REL_TOPIC, user, name,
	                             error_if_not_exists));
}

static inline Sub*
catalog_find_sub(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return sub_of(rel_mgr_find(&self->rels, REL_SUBSCRIPTION, user, name,
	                           error_if_not_exists));
}
