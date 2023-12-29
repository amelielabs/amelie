
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>

Part*
part_allocate(Uuid* id_table, Uuid* id_storage)
{
	auto self = (Part*)mn_malloc(sizeof(Part));
	self->id_storage    = id_storage;
	self->id_table      = id_table;
	self->min           = INT64_MIN;
	self->max           = INT64_MAX;
	self->snapshot      = 0;
	self->indexes_count = 0;
	list_init(&self->indexes);
	list_init(&self->link);
	list_init(&self->link_cp);
	rbtree_init_node(&self->link_node);
	return self;
}

void
part_free(Part* self)
{
	list_foreach_safe(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_free(index);
	}
	mn_free(self);
}

void
part_open(Part* self, List* indexes)
{
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);

		// create tree index
		auto index = tree_allocate(config, self->id_table, self->id_storage);
		list_append(&self->indexes, &index->link);
		self->indexes_count++;
	}
}

Index*
part_find(Part* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		if (str_compare(&index->config->name, name))
			return index;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}

hot void
part_set(Part*        self,
         Transaction* trx,
         bool         unique,
         uint8_t*     data,
         int          data_size)
{
	auto primary = part_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, row);

	// update primary index
	auto prev = index_set(primary, trx, row);
	unguard(&row_guard);

	if (unlikely(unique && prev))
		error("unique key constraint violation");
}

hot void
part_update(Part*        self,
            Transaction* trx,
            Iterator*    it,
            uint8_t*     data,
            int          data_size)
{
	auto primary = part_primary(self);

	// todo: primary iterator only

	// allocate row
	auto row = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, row);

	// update primary index
	index_update(primary, trx, it, row);
	unguard(&row_guard);
}

hot void
part_delete(Part*        self,
            Transaction* trx,
            Iterator*    it)
{
	// todo: primary iterator only

	// update primary index
	auto primary = part_primary(self);
	index_delete(primary, trx, it);
}

void
part_delete_by(Part*        self,
               Transaction* trx,
               uint8_t*     data,
               int          data_size)
{
	auto primary = part_primary(self);

	// allocate row
	auto key = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, key);

	// delete from primary index by key
	index_delete_by(primary, trx, key);
}

hot bool
part_upsert(Part*        self,
            Transaction* trx,
            Iterator**   it,
            uint8_t*     data,
            int          data_size)
{
	auto primary = part_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, row);

	// update index
	bool updated = index_upsert(primary, trx, it, row);
	if (updated)
		unguard(&row_guard);
	return updated;
}
