
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
#include <monotone_storage.h>

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

static void
storage_mgr_save(StorageMgr* self)
{
	// dump a list of storages
	auto dump = buf_create(0);
	encode_array(dump, self->list_count);
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);

		// array
		encode_array(dump, 2);

		// storage config
		storage_config_write(storage->config, dump);

		// indexes
		encode_array(dump, storage->indexes_count);
		list_foreach(&storage->indexes)
		{
			auto index = list_at(Index, link);
			index_config_write(index->config, dump);
		}
	}

	// update and save state
	var_data_set_buf(&config()->storages, dump);
	control_save_config();
	buf_free(dump);
}

static void
storage_mgr_recover(StorageMgr* self)
{
	auto storages = &config()->storages;
	if (! var_data_is_set(storages))
		return;
	auto pos = var_data_of(storages);
	if (data_is_null(pos))
		return;

	// array
	int count;
	data_read_array(&pos, &count);
	for (int i = 0; i < count; i++)
	{
		// array
		int count;
		data_read_array(&pos, &count);

		// storage config
		auto config = storage_config_read(&pos);
		guard(config_guard, storage_config_free, config);

		// prepare storage
		auto storage = storage_allocate(config);
		list_append(&self->list, &storage->link);
		self->list_count++;

		// indexes
		data_read_array(&pos, &count);
		for (int i = 0; i < count; i++)
		{
			auto index_config = index_config_read(&pos);
			guard(config_index_guard, index_config_free, index_config);

			// todo: chose by type
			auto index = tree_allocate(index_config, &storage->config->id);
			storage_attach(storage, index);
		}

		// reusing config
		unguard(&config_guard);
	}
}

void
storage_mgr_open(StorageMgr* self)
{
	// recover storages objects
	storage_mgr_recover(self);
}

void
storage_mgr_gc(StorageMgr* self)
{
	bool updated = false;
	list_foreach_safe(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (storage->refs > 0)
			continue;
		list_unlink(&storage->link);
		self->list_count--;
		storage_free(storage);
		updated = true;
	}

	if (updated)
		storage_mgr_save(self);
}

Buf*
storage_mgr_list(StorageMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);

	// map
	encode_map(buf, self->list_count);
	list_foreach(&self->list)
	{
		// id: {}
		auto storage = list_at(Storage, link);

		char uuid[UUID_SZ];
		uuid_to_string(&storage->config->id, uuid, sizeof(uuid));
		encode_raw(buf, uuid, sizeof(uuid) - 1);

		// map
		encode_map(buf, 3);

		// refs
		encode_raw(buf, "refs", 3);
		encode_integer(buf, storage->refs);

		// config
		encode_raw(buf, "id", 2);
		storage_config_write(storage->config, buf);

		// engine
		encode_raw(buf, "index", 5);
		encode_array(buf, storage->indexes_count);
		list_foreach(&storage->indexes)
		{
			auto index = list_at(Index, link);
			index_config_write(index->config, buf);
		}
	}

	msg_end(buf);
	return buf;
}

Storage*
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

Storage*
storage_mgr_find_for(StorageMgr* self, Uuid* shard, Uuid* table)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (uuid_compare(&storage->config->id_shard, shard) &&
		    uuid_compare(&storage->config->id_table, table))
			return storage;
	}
	return NULL;
}


Storage*
storage_mgr_create(StorageMgr* self, StorageConfig* config)
{
	auto storage = storage_allocate(config);
	list_append(&self->list, &storage->link);
	self->list_count++;

	storage_mgr_save(self);
	return storage;
}

void
storage_mgr_attach(StorageMgr* self, Storage* storage, Index* index)
{
	storage_attach(storage, index);
	storage_mgr_save(self);
}

void
storage_mgr_detach(StorageMgr* self, Storage* storage, Index* index)
{
	storage_detach(storage, index);
	storage_mgr_save(self);
}
