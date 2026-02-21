
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>
#include <amelie_catalog.h>

void
catalog_snapshot(Catalog* self, Buf* data)
{
	// catalog
	encode_raw(data, "catalog", 7);
	auto lsn = state_catalog_pending();
	auto buf = catalog_write_prepare(self, lsn);
	defer_buf(buf);
	buf_write_buf(data, buf);

	// storage (directories)
	encode_raw(data, "storage", 7);
	encode_array(data);
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		list_foreach(&table->config->tiers)
		{
			auto tier = list_at(Tier, link);
			list_foreach(&tier->storages)
			{
				auto tier_storage = list_at(TierStorage, link);
				char id[UUID_SZ];
				uuid_get(&tier_storage->id, id, sizeof(id));
				char path[256];
				auto path_size = sfmt(path, sizeof(path), "storage/%s", id);
				encode_raw(data, path, path_size);
			}
		}
	}
	encode_array_end(data);

	// partitions
	encode_raw(data, "partitions", 10);
	encode_array(data);

	// create partitions files snapshots (hard links)
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		auto table_lock = lock(&table->rel, LOCK_SHARED);
		defer(unlock, table_lock);

		list_foreach(&engine_main(&table->engine)->list_ram)
		{
			auto part = list_at(Part, id.link);
			id_snapshot(&part->id, ID_RAM, ID_RAM_SNAPSHOT);
			// [path, size, mode]
			id_encode(&part->id, ID_RAM_SNAPSHOT, data);
		}
	}

	encode_array_end(data);
}

void
catalog_snapshot_cleanup(Buf* data)
{
	auto pos = data->start;
	uint8_t* pos_partitions = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "partitions", &pos_partitions },
		{ 0,             NULL,         NULL           },
	};
	decode_obj(obj, "snapshot", &pos);

	// drop partitions snapshots files (hard links)
	json_read_array(&pos_partitions);
	while (! json_read_array_end(&pos_partitions))
	{
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_partitions, &path_relative, &size, &mode);
		if (! fs_exists("%s/%.*s", state_directory(), str_size(&path_relative),
		                str_of(&path_relative)))
			continue;
		fs_unlink("%s/%.*s", state_directory(), str_size(&path_relative),
		          str_of(&path_relative));
	}
}
