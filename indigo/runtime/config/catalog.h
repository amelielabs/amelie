#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct CatalogIf CatalogIf;
typedef struct Catalog   Catalog;

struct CatalogIf
{
	void (*dump)(Catalog*, Buf*);
	void (*restore)(Catalog*, uint64_t, uint8_t**);
};

struct Catalog
{
	Str        name;
	CatalogIf* iface;
	void*      iface_arg;
	List       link;
};

static inline Catalog*
catalog_allocate(char* name, CatalogIf* iface, void* iface_arg)
{
	auto self = (Catalog*)mn_malloc(sizeof(Catalog));
	self->iface     = iface;
	self->iface_arg = iface_arg;
	str_set_cstr(&self->name, name);
	list_init(&self->link);
	return self;
}

static inline void
catalog_free(Catalog* self)
{
	mn_free(self);
}
