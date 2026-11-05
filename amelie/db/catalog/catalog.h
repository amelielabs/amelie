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

struct CatalogIf
{
	void (*udf_compile)(Catalog*, Udf*);
	void (*udf_free)(Udf*);
	bool (*udf_depends)(Udf*, Str*);
};

struct Catalog
{
	Rels       rels;
	Usage*     memory;
	CatalogIf* iface;
	void*      iface_arg;
};

void catalog_init(Catalog*, CatalogIf*, void*, Usage*);
void catalog_free(Catalog*);
void catalog_limit(Catalog*, Tr*, RelType, int);
bool catalog_execute(Catalog*, Tr*, uint8_t*, int);

static inline Rel*
catalog_find(Catalog* self, RelType type, Str* name, bool error_if_not_exists)
{
	return rels_find(&self->rels, type, name, error_if_not_exists);
}

static inline Table*
catalog_find_table(Catalog* self, Str* name, bool error_if_not_exists)
{
	return table_of(rels_find(&self->rels, REL_TABLE, name, error_if_not_exists));
}

static inline Udf*
catalog_find_udf(Catalog* self, Str* name, bool error_if_not_exists)
{
	return udf_of(rels_find(&self->rels, REL_UDF, name, error_if_not_exists));
}
