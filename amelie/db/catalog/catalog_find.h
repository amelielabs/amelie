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

static inline User*
catalog_find_user(Catalog* self, Str* name, bool error_if_not_exists)
{
	return user_of(rels_find(&self->users, REL_USER, NULL, name,
	                         error_if_not_exists));
}

static inline Rel*
catalog_find(Catalog* self, RelType type, Str* user, Str* name, bool error_if_not_exists)
{
	return rels_find(&self->rels, type, user, name,
	                 error_if_not_exists);
}

static inline Rel*
catalog_find_by(Catalog* self, RelType type, Uuid* id, bool error_if_not_exists)
{
	return rels_find_by(&self->rels, type, id, error_if_not_exists);
}

static inline Table*
catalog_find_table(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return table_of(rels_find(&self->rels, REL_TABLE, user, name,
	                          error_if_not_exists));
}

static inline Clone*
catalog_find_clone(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return clone_of(rels_find(&self->rels, REL_CLONE, user, name,
	                          error_if_not_exists));
}

static inline Udf*
catalog_find_udf(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return udf_of(rels_find(&self->rels, REL_UDF, user, name,
	                        error_if_not_exists));
}

static inline Topic*
catalog_find_topic(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return topic_of(rels_find(&self->rels, REL_TOPIC, user, name,
	                          error_if_not_exists));
}

static inline Sub*
catalog_find_sub(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	return sub_of(rels_find(&self->rels, REL_SUBSCRIPTION, user, name,
	                        error_if_not_exists));
}
