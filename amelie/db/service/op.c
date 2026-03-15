
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>

void
service_refresh(Service* self, Uuid* id_table, uint64_t id, Str* storage)
{
	// note: executed under shared catalog lock
	auto table = table_mgr_find_by(&self->catalog->table_mgr, id_table, true);
	Refresh refresh;
	refresh_init(&refresh, self);
	defer(refresh_free, &refresh);
	if (! refresh_run(&refresh, table, id, storage))
		error("partition or storage not found");
}

void
service_checkpoint(Service* self)
{
	// note: executed under shared catalog lock
	auto lsn = state_lsn();

	// update catalog file if there are pending ddls
	if (state_catalog() < state_catalog_pending())
		catalog_write(self->catalog);

	// refresh partitions
	Refresh refresh;
	refresh_init(&refresh, self);
	defer(refresh_free, &refresh);

	list_foreach(&self->catalog->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		for (;;)
		{
			uint64_t id = UINT64_MAX;
			auto table_lock = lock(&table->rel, LOCK_SHARED);
			list_foreach(&table->part_mgr.list)
			{
				auto part = list_at(Part, link);
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

	service_gc(self);
}

void
service_gc(Service* self)
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
	list_foreach(&self->catalog->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		auto table_lock = lock(&table->rel, LOCK_SHARED);
		list_foreach(&table->part_mgr.list)
		{
			auto part = list_at(Part, link);
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
	wal_gc(self->wal, lsn);
}

static void
service_sync_job(intptr_t* argv)
{
	auto service = (Service*)argv[0];
	auto id      = argv[1];
	auto close   = argv[2];
	auto file = wal_find(service->wal, id, false);
	if (! file)
		return;
	defer(wal_file_unpin_defer, file);
	wal_file_sync(file);
	if (close)
		wal_file_close(file);
}

void
service_sync(Service* self, uint64_t id, bool close)
{
	run(service_sync_job, 3, self, id, close);
}

static void
service_execute(Service* self, Action* action)
{
	switch (action->type) {
	case ACTION_SYNC:
		service_sync(self, action->id, false);
		break;
	case ACTION_SYNC_CLOSE:
		service_sync(self, action->id, true);
		break;
	case ACTION_CHECKPOINT:
		service_checkpoint(self);
		break;
	}
}

bool
service_step(Service* self)
{
	auto action = action_mgr_next(&self->action_mgr);
	if (! action)
		return false;
	error_catch (
		service_execute(self, action);
	);
	action_mgr_complete(&self->action_mgr, action);
	return true;
}
