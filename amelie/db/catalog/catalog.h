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
	StorageMgr storage_mgr;
	DbMgr      db_mgr;
	TableMgr   table_mgr;
	UdfMgr     udf_mgr;
	CatalogIf* iface;
	void*      iface_arg;
};

void catalog_init(Catalog*, CatalogIf*, void*, Deploy*);
void catalog_free(Catalog*);
void catalog_open(Catalog*, bool);
void catalog_close(Catalog*);
bool catalog_execute(Catalog*, Tr*, uint8_t*, int);
Buf* catalog_status(Catalog*);
