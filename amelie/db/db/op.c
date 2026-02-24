
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
#include <amelie_wal.h>
#include <amelie_db.h>

void
db_refresh(Db* self, Uuid* id_table, uint64_t id, Str* storage)
{
	auto table = table_mgr_find_by(&self->catalog.table_mgr, id_table, true);
	Refresh refresh;
	refresh_init(&refresh, self);
	defer(refresh_free, &refresh);
	if (! refresh_run(&refresh, table, id, storage))
		error("partition or storage not found");
}

void
db_flush(Db* self, Uuid* id_table, uint64_t id)
{
	auto table = table_mgr_find_by(&self->catalog.table_mgr, id_table, true);
	Flush flush;
	flush_init(&flush, self);
	defer(flush_free, &flush);
	if (! flush_run(&flush, table, id))
		error("partition not found");
}

void
db_checkpoint(Db* self)
{
	// note: executed under shared catalog lock
	auto lsn = state_lsn();

	// update catalog file if there are pending ddls
	if (state_catalog() < state_catalog_pending())
		catalog_write(&self->catalog);

	// refresh partitions
	Refresh refresh;
	refresh_init(&refresh, self);
	defer(refresh_free, &refresh);

	list_foreach(&self->catalog.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		for (;;)
		{
			uint64_t id = UINT64_MAX;
			auto table_lock = lock(&table->rel, LOCK_SHARED);
			list_foreach(&table->part_mgr.list)
			{
				auto part = list_at(Part, id.link);
				if (part->heap->header->lsn < lsn && part_has_updates(part))
				{
					id = part->id.id;
					break;
				}
			}
			unlock(table_lock);

			if (id == UINT64_MAX)
				break;

			refresh_run(&refresh, table, id, NULL);
		}
	}

	// wal gc
	db_gc(self);
}

void
db_gc(Db* self)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);

	// include catalog lsn if there are any pending catalog
	// operations in the wal
	//
	// since pending catalog sets to max ddl lsn, we are using
	// catalog lsn as min
	//
	auto lsn             = UINT64_MAX;
	auto catalog         = state_catalog();
	auto catalog_pending = state_catalog_pending();
	if (catalog_pending > catalog)
		lsn = catalog;

	// calculate min lsn accross partitions files (merged)
	list_foreach(&self->catalog.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		auto table_lock = lock(&table->rel, LOCK_SHARED);
		list_foreach(&table->part_mgr.list)
		{
			auto part = list_at(Part, id.link);
			// include partition only if it has pending updates
			if (! part_has_updates(part))
				continue;
			auto part_lsn = part->heap->header->lsn;
			if (part_lsn < lsn)
				lsn = part_lsn;
		}
		unlock(table_lock);
	}

	unlock(lock_catalog);

	// remove wal files < lsn
	wal_gc(&self->wal_mgr.wal, lsn);
}

Snapshot*
db_snapshot(Db* self)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	defer(unlock, lock_catalog);
	return snapshot_mgr_create(&self->snapshot_mgr);
}

void
db_snapshot_drop(Db* self, Snapshot* snapshot)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	error_catch (
		snapshot_mgr_drop(&self->snapshot_mgr, snapshot);
	);
	unlock(lock_catalog);

	// wal gc
	db_gc(self);
}
