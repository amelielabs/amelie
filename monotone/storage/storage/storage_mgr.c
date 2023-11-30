
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_index.h>
#include <monotone_storage.h>

void
storage_mgr_init(StorageMgr* self)
{
	self->list_count = 0;
	self->map        = NULL;
	list_init(&self->list);
}

void
storage_mgr_free(StorageMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto storage = list_at(Storage, link);
		storage_free(storage);
	}
	self->list_count = 0;
	list_init(&self->list);
}

void
storage_mgr_open(StorageMgr* self,
                 Mapping*    map,
                 Uuid*       table,
                 List*       storages,
                 List*       indexes)
{
	self->map = map;

	// prepare storages and previously saved partitions
	list_foreach(storages)
	{
		auto config = list_at(StorageConfig, link);

		// prepare storage
		auto storage = storage_allocate(config, self->map, table);
		list_append(&self->list, &storage->link);
		self->list_count++;

		// prepare storage partitions
		storage_open(storage, indexes);
	}
}

hot Storage*
storage_mgr_find(StorageMgr* self, Uuid* shard)
{
	if (self->map->type == MAP_REFERENCE)
	{
		assert(self->list_count == 1);
		return container_of(self->list.next, Storage, link);
	}
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (uuid_compare(&storage->config->shard, shard))
			return storage;
	}
	return NULL;
}
