
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
	compact_mgr_stop(&self->compact_mgr);
}

void
storage_mgr_open(StorageMgr* self)
{
	// start workers
	compact_mgr_start(&self->compact_mgr, var_int_of(&config()->engine_workers));
}

void
storage_mgr_gc(StorageMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto storage = list_at(Storage, link);
		if (storage->refs > 0)
			continue;
		list_unlink(&storage->link);
		self->list_count--;
		storage_free(storage);
	}
}

Storage*
storage_mgr_create(StorageMgr* self, StorageConfig* config)
{
	return storage_create(config, &self->compact_mgr);
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
