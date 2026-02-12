
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
	// config
	encode_raw(data, "config", 6);
	auto buf = opts_list_persistent(&runtime()->config.opts);
	defer_buf(buf);
	buf_write_buf(data, buf);

	// state
	encode_raw(data, "state", 5);
	buf = opts_list_persistent(&runtime()->state.opts);
	defer_buf(buf);
	buf_write_buf(data, buf);

	// catalog
	encode_raw(data, "catalog", 7);
	auto lsn = state_catalog_pending();
	buf = catalog_write_prepare(self, lsn);
	defer_buf(buf);
	buf_write_buf(data, buf);

	// partitions
	encode_raw(data, "partitions", 10);
	encode_array(data);

	// create partitions files snapshots
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		auto table_lock = lock(&table->rel, LOCK_SHARED);
		defer(unlock, table_lock);

		// create <storage_path>/snapshot/<id> hard link
		list_foreach(&engine_main(&table->engine)->list)
		{
			auto part = list_at(Part, link);
			id_snapshot(&part->id, ID_RAM);
			id_encode(&part->id, ID_RAM, data);
		}
	}

	encode_array_end(data);
}

void
catalog_snapshot_cleanup(Catalog* self)
{
	list_foreach(&self->storage_mgr.mgr.list)
	{
		auto storage = storage_of(list_at(Relation, link));
		storage_snapshot_cleanup(storage);
	}
}
