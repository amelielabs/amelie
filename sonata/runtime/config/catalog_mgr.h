#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct CatalogMgr CatalogMgr;

struct CatalogMgr
{
	List list;
	int  list_count;
};

void catalog_mgr_init(CatalogMgr*);
void catalog_mgr_free(CatalogMgr*);
void catalog_mgr_add(CatalogMgr*, Catalog*);
Buf* catalog_mgr_dump(CatalogMgr*);
void catalog_mgr_restore(CatalogMgr*, uint8_t**);
