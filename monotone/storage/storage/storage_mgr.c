
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
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>
#include <monotone_storage.h>

void
storage_mgr_init(StorageMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
	compact_mgr_init(&self->compact_mgr);
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

	compact_mgr_stop(&self->compact_mgr);
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
		storage_config_write(storage->config, dump);
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

	int count;
	data_read_array(&pos, &count);
	for (int i = 0; i < count; i++)
	{
		// value
		auto config = storage_config_read(&pos);
		guard(config_guard, storage_config_free, config);

		auto storage = storage_allocate(config, &self->compact_mgr);
		list_append(&self->list, &storage->link);
		self->list_count++;
	}
}

void
storage_mgr_open(StorageMgr* self)
{
	// recover storages objects
	storage_mgr_recover(self);

	// start workers
	compact_mgr_start(&self->compact_mgr, var_int_of(&config()->engine_workers));
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

void
storage_mgr_assign(StorageMgr* self, StorageList* list, Uuid* id_shard)
{
	list_foreach_safe(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (! uuid_compare(&storage->config->id_shard, id_shard))
			continue;
		storage_list_add(list, storage);
	}
}

Storage*
storage_mgr_create(StorageMgr* self, StorageConfig* config)
{
	auto storage = storage_allocate(config, &self->compact_mgr);
	list_append(&self->list, &storage->link);
	self->list_count++;

	storage_mgr_save(self);
	return storage;
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

Buf*
storage_mgr_show(StorageMgr* self)
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
		encode_raw(buf, "engine", 6);
		EngineStats stats;
		engine_stats_get(&storage->engine, &stats);
		engine_stats_write(&stats, buf);
	}

	msg_end(buf);
	return buf;
}
