
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
	self->reference  = false;
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
storage_mgr_open(StorageMgr* self, bool reference, List* storages, List* indexes)
{
	self->reference = reference;

	list_foreach(storages)
	{
		auto config = list_at(StorageConfig, link);
		config_ssn_follow(config->id);

		// prepare storage
		auto storage = storage_allocate(config);
		list_append(&self->list, &storage->link);
		self->list_count++;

		// prepare indexes
		storage_open(storage, indexes);
	}
}

hot Storage*
storage_mgr_find(StorageMgr* self, uint64_t id)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (storage->config->id == (int64_t)id)
			return storage;
	}
	return NULL;
}

hot Storage*
storage_mgr_match(StorageMgr* self, Uuid* shard)
{
	// get first storage, if reference or find storage by shard id
	if (self->reference)
	{
		auto first = list_first(&self->list);
		return container_of(first, Storage, link);
	}

	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (! uuid_compare(&storage->config->shard, shard))
			continue;
		return storage;
	}
	return NULL;
}
