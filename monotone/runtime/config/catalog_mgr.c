
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>

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

static Buf*
catalog_mgr_dump_create(CatalogMgr* self)
{
	auto buf = buf_create(0);
	encode_map(buf, self->list_count);
	list_foreach(&self->list)
   	{
		auto cat = list_at(Catalog, link);
		encode_string(buf, &cat->name);
		cat->iface->dump(cat, buf);
	}
	return buf;
}

void
catalog_mgr_dump(CatalogMgr* self, uint64_t lsn)
{
	auto dump = catalog_mgr_dump_create(self);

	// catalog_snapshot
	var_int_set(&config()->catalog_snapshot, lsn);

	// catalog
	var_data_set_buf(&config()->catalog, dump);

	control_save_state();
	buf_free(dump);
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
catalog_mgr_restore(CatalogMgr* self)
{
	auto cp = &config()->catalog;
	if (! var_data_is_set(cp))
		return;
	auto data = var_data_of(cp);
	if (data_is_null(data))
		return;

	// restore catalogs
	uint64_t lsn = var_int_of(&config()->catalog_snapshot);

	// map
	uint8_t* pos = data;
	int count;
	data_read_map(&pos, &count);
	while (count-- > 0)
	{
		// catalog
		Str name;
		data_read_string(&pos, &name);

		auto cat = catalog_mgr_find(self, &name);
		if (unlikely(cat == NULL))
			error("catalog: <%.*s> not found", str_size(&name),
			      str_of(&name));
		cat->iface->restore(cat, lsn, &pos);
	}
}
