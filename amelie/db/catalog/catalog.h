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

typedef struct CatalogIf CatalogIf;
typedef struct Catalog   Catalog;
typedef struct Table     Table;
typedef struct Udf       Udf;
typedef struct Database  Database;

struct CatalogIf
{
	void (*udf_compile)(Catalog*, Udf*);
	void (*udf_free)(Udf*);
	bool (*udf_depends)(Udf*, Str*);
	void (*attach)(Catalog*, Database*);
	void (*detach)(Catalog*, Database*);
};

struct Catalog
{
	Rels       rels;
	Usage*     memory;
	Str*       database;
	CatalogIf* iface;
	void*      iface_arg;
};

static inline void
catalog_init(Catalog*   self,
             CatalogIf* iface,
             void*      iface_arg,
             Usage*     memory,
             Str*       database)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->memory    = memory;
	self->database  = database;
	rels_init(&self->rels);
}

static inline void
catalog_free(Catalog* self)
{
	rels_free(&self->rels);
}

static inline void
catalog_limit(Catalog* self, Tr* tr, RelType type, int limit_id)
{
	if (!tr->local || !tr->local->limits)
		return;

	auto limits = tr->local->limits;
	if (! limits_is_set(limits, limit_id))
		return;

	auto count = rels_count(&self->rels, type);
	if (unlikely(! limits_check(limits, limit_id, count + 1)))
		error("{s}s: limit reached", rel_type_of(type));
}

static inline Rel*
catalog_find(Catalog* self, RelType type, Str* name, bool error_if_not_exists)
{
	return rels_find(&self->rels, type, name, error_if_not_exists);
}

static inline Table*
catalog_find_table(Catalog* self, Str* name, bool error_if_not_exists)
{
	return (Table*)rels_find(&self->rels, REL_TABLE, name, error_if_not_exists);
}

static inline Udf*
catalog_find_udf(Catalog* self, Str* name, bool error_if_not_exists)
{
	return (Udf*)rels_find(&self->rels, REL_UDF, name, error_if_not_exists);
}

static inline Database*
catalog_find_database(Catalog* self, Str* name, bool error_if_not_exists)
{
	return (Database*)rels_find(&self->rels, REL_DATABASE, name,
	                            error_if_not_exists);
}
