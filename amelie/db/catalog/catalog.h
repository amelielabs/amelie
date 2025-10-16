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
	void (*build_index)(Catalog*, Table*, IndexConfig*);
	void (*build_column_add)(Catalog*, Table*, Table*, Column*);
	void (*build_column_drop)(Catalog*, Table*, Table*, Column*);
	void (*proc_compile)(Catalog*, Proc*);
	void (*proc_free)(Proc*);
	bool (*proc_depends)(Proc*, Str*, Str*);
};

struct Catalog
{
	SchemaMgr  schema_mgr;
	TableMgr   table_mgr;
	ProcMgr    proc_mgr;
	CatalogIf* iface;
	void*      iface_arg;
};

void catalog_init(Catalog*, PartMgr*, CatalogIf*, void*);
void catalog_free(Catalog*);
void catalog_open(Catalog*);
void catalog_close(Catalog*);
bool catalog_execute(Catalog*, Tr*, uint8_t*, int);
Buf* catalog_status(Catalog*);
