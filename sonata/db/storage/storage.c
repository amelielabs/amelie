
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
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>

Storage*
storage_allocate(StorageConfig* config)
{
	auto self = (Storage*)so_malloc(sizeof(Storage));
	self->config        = NULL;
	self->indexes_count = 0;
	list_init(&self->indexes);
	list_init(&self->link_cp);
	list_init(&self->link);
	guard(storage_free, self);
	self->config = storage_config_copy(config);
	unguard();
	return self;
}

void
storage_free(Storage* self)
{
	list_foreach_safe(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_free(index);
	}
	storage_config_free(self->config);
	so_free(self);
}

void
storage_open(Storage* self, List* indexes)
{
	// recreate indexes
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);
		auto index  = tree_allocate(config, self->config->id);
		list_append(&self->indexes, &index->link);
		self->indexes_count++;
	}
}

hot void
storage_ingest(Storage* self, uint8_t** pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// update primary index
	index_ingest(primary, row);
	unguard();

	// todo: secondary indexes
}

hot void
storage_set(Storage*     self,
            Transaction* trx,
            bool         unique,
            uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// update primary index
	auto prev = index_set(primary, trx, row);
	unguard();

	if (unlikely(unique && prev))
		error("unique key constraint violation");
}

hot void
storage_update(Storage*     self,
               Transaction* trx,
               Iterator*    it,
               uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// todo: primary iterator only

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// update primary index
	index_update(primary, trx, it, row);
	unguard();
}

hot void
storage_delete(Storage*     self,
               Transaction* trx,
               Iterator*    it)
{
	// todo: primary iterator only

	// update primary index
	auto primary = storage_primary(self);
	index_delete(primary, trx, it);
}

void
storage_delete_by(Storage*     self,
                  Transaction* trx,
                  uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto key = row_create(&primary->config->def, pos);
	guard(row_free, key);

	// delete from primary index by key
	index_delete_by(primary, trx, key);
}

hot void
storage_upsert(Storage*     self,
               Transaction* trx,
               Iterator**   it,
               uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// do insert or return iterator
	index_upsert(primary, trx, it, row);
	if (*it == NULL)
		unguard();
}

Index*
storage_find(Storage* self, Str* name, bool error_if_not_exists)
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
