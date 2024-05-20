
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

void
storage_mgr_init(StorageMgr* self)
{
	self->list_count = 0;
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
                 Uuid*       table,
                 List*       storages,
                 List*       indexes)
{
	list_foreach(storages)
	{
		auto config = list_at(StorageConfig, link);

		// prepare storage
		auto storage = storage_allocate(config, table);
		list_append(&self->list, &storage->link);
		self->list_count++;

		// prepare indexes and read storage directory
		storage_open(storage, indexes);
	}
}

static void
storage_mgr_recover_reference(StorageMgr* self)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (storage_is_reference(storage))
		{
			storage_recover(storage);
			break;
		}
	}
}

void
storage_mgr_recover(StorageMgr* self, Uuid* shard)
{
	// recover reference storage (shard is NULL) or storages
	// which match the specified shard
	if (shard == NULL)
	{
		storage_mgr_recover_reference(self);
		return;
	}

	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (! uuid_compare(&storage->config->shard, shard))
			continue;
		storage_recover(storage);
	}
}

void
storage_mgr_gc(StorageMgr* self)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		snapshot_mgr_gc(&storage->snapshot_mgr);
	}
}

hot Storage*
storage_mgr_find(StorageMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (uuid_compare(&storage->config->id, id))
			return storage;
	}
	return NULL;
}

hot Storage*
storage_mgr_find_by_shard(StorageMgr* self, Uuid* shard)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (storage_is_reference(storage))
			return storage;
		if (uuid_compare(&storage->config->shard, shard))
			return storage;
	}
	return NULL;
}
