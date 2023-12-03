
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

void
storage_mgr_list(StorageMgr* self, Buf* buf)
{
	// array
	encode_array(buf, self->list_count);
	list_foreach_safe(&self->list)
	{
		auto storage = list_at(Storage, link);
		guard(unlock, mutex_unlock, &storage->list_dump_lock);

		// []
		encode_array(buf, 2);

		// storage_config
		storage_config_write(storage->config, buf);

		// partitions
		buf_write(buf, storage->list_dump.start, buf_size(&storage->list_dump));
	}
}

hot void
storage_mgr_snapshot(StorageMgr*     self,
                     SnapshotWriter* writer,
                     Snapshot*       snapshot,
                     uint64_t        lsn)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		for (;;)
		{
			auto part = storage_schedule(storage, lsn);
			if (! part)
				break;

			part_snapshot(part, writer, snapshot, lsn);
		}
	}
}

hot Storage*
storage_mgr_find(StorageMgr* self, Uuid* id)
{
	if (self->map->type == MAP_REFERENCE)
	{
		assert(self->list_count == 1);
		return container_of(self->list.next, Storage, link);
	}
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
