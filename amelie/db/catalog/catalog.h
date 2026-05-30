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
	void (*user_invalidate)(Catalog*, User*);
	void (*udf_compile)(Catalog*, Udf*);
	void (*udf_free)(Udf*);
	bool (*udf_depends)(Udf*, Str*, Str*);
};

struct Catalog
{
	RelMgr     users;
	RelMgr     rels;
	Columns    topic_columns;
	Columns    cdc_columns;
	Cdc*       cdc;
	PartMgrIf* iface_part;
	void*      iface_part_arg;
	CatalogIf* iface;
	void*      iface_arg;
};

void catalog_init(Catalog*, CatalogIf*, void*, PartMgrIf*, void*, Cdc*);
void catalog_free(Catalog*);
void catalog_open(Catalog*, bool);
void catalog_close(Catalog*);
bool catalog_execute(Catalog*, Tr*, uint8_t*, int);
void catalog_status(Catalog*, Buf*);
