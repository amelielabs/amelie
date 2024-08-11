#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct CatalogIf CatalogIf;
typedef struct Catalog   Catalog;

struct CatalogIf
{
	void (*dump)(Catalog*, Buf*);
	void (*restore)(Catalog*, uint8_t**);
};

struct Catalog
{
	CatalogIf* iface;
	void*      iface_arg;
};

static inline void
catalog_init(Catalog* self, CatalogIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
}

static inline Buf*
catalog_dump(Catalog* self)
{
	auto buf = buf_begin();
	self->iface->dump(self, buf);
	return buf_end(buf);
}

static inline void
catalog_restore(Catalog* self, uint8_t** pos)
{
	self->iface->restore(self, pos);
}
