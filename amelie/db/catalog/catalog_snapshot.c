
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
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
	auto tsn = state_tsn();
	auto buf = catalog_write_prepare(self, lsn, tsn);
	defer_buf(buf);
	buf_write_buf(data, buf);

	// storages
	encode_raw(data, "storages", 8);
	encode_array(data);
	list_foreach(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_TABLE)
			continue;
		auto table = table_of(rel);
		char id[UUID_SZ];
		uuid_get(&table->config->id, id, sizeof(id));
		char path[256];
		auto path_size = format(path, sizeof(path), "storage/{s}", id);
		encode_raw(data, path, path_size);
	}
	encode_array_end(data);

	// files
	encode_raw(data, "files", 5);
	encode_array(data);

	// create snapshot files (hard links)
	list_foreach(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_TABLE)
			continue;
		auto table = table_of(rel);
		auto table_lock = lock(&table->rel, LOCK_SHARED);
		defer(unlock, table_lock);

		// partitions
		list_foreach(&table->part_mgr.list)
		{
			auto part = list_at(Part, link);
			id_snapshot(&part->id, ID_PARTITION, ID_PARTITION_SNAPSHOT);
			id_encode(&part->id, ID_PARTITION_SNAPSHOT, data);
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
	unpack_array(&pos_files);
	while (! unpack_array_end(&pos_files))
	{
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_files, &path_relative, &size, &mode);
		if (! fs_exists("{s}/{str}", state_directory(), &path_relative))
			continue;
		fs_unlink("{s}/{str}", state_directory(), &path_relative);
	}
}
