
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
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
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

	// volumes
	encode_raw(data, "volumes", 7);
	encode_array(data);
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		volume_mgr_list(&table->config->part_mgr_config.volumes, data);
		list_foreach(&table->config->tiers)
		{
			auto config = list_at(TierConfig, link);
			volume_mgr_list(&config->volumes, data);
		}
	}
	encode_array_end(data);

	// files
	encode_raw(data, "files", 5);
	encode_array(data);

	// create partitions files snapshots (hard links)
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		auto table_lock = lock(&table->rel, LOCK_SHARED);
		defer(unlock, table_lock);

		// partitions
		list_foreach(&table->part_mgr.list)
		{
			auto id = list_at(Id, link);
			auto id_snap = id_snapshot_of(id->type);
			id_snapshot(id, id->type, id_snap);
			id_encode(id, id_snap, data);
		}

		// objects
		list_foreach(&table->tier_mgr.list)
		{
			auto tier = list_at(Tier, link);
			list_foreach(&tier->list)
			{
				auto id = list_at(Id, link);
				auto id_snap = id_snapshot_of(id->type);
				id_snapshot(id, id->type, id_snap);
				id_encode(id, id_snap, data);
			}
		}
	}

	encode_array_end(data);
}

void
catalog_snapshot_cleanup(Buf* data)
{
	auto pos = data->start;
	uint8_t* pos_files = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "files", &pos_files },
		{ 0,             NULL,    NULL      },
	};
	decode_obj(obj, "snapshot", &pos);

	// drop snapshots files (hard links)
	json_read_array(&pos_files);
	while (! json_read_array_end(&pos_files))
	{
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_files, &path_relative, &size, &mode);
		if (! fs_exists("%s/%.*s", state_directory(), str_size(&path_relative),
		                str_of(&path_relative)))
			continue;
		fs_unlink("%s/%.*s", state_directory(), str_size(&path_relative),
		          str_of(&path_relative));
	}
}
