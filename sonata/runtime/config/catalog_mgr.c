
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>

void
catalog_mgr_init(CatalogMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
catalog_mgr_free(CatalogMgr* self)
{
	list_foreach_safe(&self->list) {
		auto cat = list_at(Catalog, link);
		catalog_free(cat);
	}
}

void
catalog_mgr_add(CatalogMgr* self, Catalog* cat)
{
	list_append(&self->list, &cat->link);
	self->list_count++;
}

Buf*
catalog_mgr_dump(CatalogMgr* self)
{
	auto buf = buf_begin();
	encode_map(buf, self->list_count);
	list_foreach(&self->list)
   	{
		auto cat = list_at(Catalog, link);
		encode_string(buf, &cat->name);
		cat->iface->dump(cat, buf);
	}
	return buf_end(buf);
}

static Catalog*
catalog_mgr_find(CatalogMgr* self, Str* name)
{
	list_foreach(&self->list) {
		auto cat = list_at(Catalog, link);
		if (str_compare(&cat->name, name))
			return cat;
	}
	return NULL;
}

void
catalog_mgr_restore(CatalogMgr* self, uint8_t** pos)
{
	int count;
	data_read_map(pos, &count);
	while (count-- > 0)
	{
		// catalog
		Str name;
		data_read_string(pos, &name);
		auto cat = catalog_mgr_find(self, &name);
		if (unlikely(cat == NULL))
			error("catalog: <%.*s> not found", str_size(&name),
			      str_of(&name));
		cat->iface->restore(cat, pos);
	}
}
