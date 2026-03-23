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
	bool (*udf_depends)(Udf*, Str*);
};

struct Catalog
{
	UserMgr     user_mgr;
	StorageMgr  storage_mgr;
	TableMgr    table_mgr;
	UdfMgr      udf_mgr;
	SynonymMgr  synonym_mgr;
	CatalogIf*  iface;
	void*       iface_arg;
};

void   catalog_init(Catalog*, CatalogIf*, void*, PartMgrIf*, void*);
void   catalog_free(Catalog*);
void   catalog_open(Catalog*, bool);
void   catalog_close(Catalog*);
bool   catalog_execute(Catalog*, Tr*, uint8_t*, int);
Buf*   catalog_status(Catalog*);
Rel*   catalog_find(Catalog*, Str*, Str*, bool);
Table* catalog_find_table(Catalog*, Str*, Str*, bool);
