
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
#include <amelie_tier>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
storage_refresh(Storage* self, Refresh* refresh, Id* id, Str* tier)
{
	unused(self);
	refresh_run(refresh, id, tier);
}

static Buf*
storage_pending(Storage* self)
{
	auto lock_mgr = &self->lock_mgr;

	// create a list of all pending partitions
	auto list = buf_create();
	errdefer_buf(list);
	list_foreach(&self->catalog.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		lock(lock_mgr, &table->rel, LOCK_SHARED);

		list_foreach(&table->volume_mgr.parts)
		{
			auto part = list_at(Part, link);
			if (part_has_updates(part))
				buf_write(list, &part->id, sizeof(part->id));
		}

		unlock(lock_mgr, &table->rel, LOCK_SHARED);
	}
	return list;
}

void
storage_checkpoint(Storage* self)
{
	auto lock_mgr = &self->lock_mgr;
	lock_checkpoint(lock_mgr, LOCK_EXCLUSIVE);
	lock_catalog(lock_mgr, LOCK_SHARED);

	Buf* list = NULL;
	auto on_error = error_catch
	(
		// update catalog file if there are pending ddls
		if (state_catalog() < state_catalog_pending())
			catalog_write(&self->catalog);

		// create a list of all pending partitions
		list = storage_pending(self);
	);
	unlock_catalog(lock_mgr, LOCK_SHARED);
	unlock_checkpoint(lock_mgr, LOCK_EXCLUSIVE);

	if (on_error)
		rethrow();

	defer_buf(list);

	// refresh partitions
	Refresh refresh;
	refresh_init(&refresh, self);
	defer(refresh_free, &refresh);
	auto end = (Id*)list->position;
	auto it  = (Id*)list->start;
	for (; it < end; it++)
		storage_refresh(self, &refresh, it, NULL);

	// wal gc
	storage_gc(self);
}

void
storage_gc(Storage* self)
{
	// taking exclusive checkpoint lock to prevent
	// the catalog lsn change
	auto lock_mgr = &self->lock_mgr;
	lock_checkpoint(lock_mgr, LOCK_EXCLUSIVE);
	lock_catalog(lock_mgr, LOCK_SHARED);

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
		lock(lock_mgr, &table->rel, LOCK_SHARED);

		list_foreach(&table->volume_mgr.parts)
		{
			auto part = list_at(Part, link);
			auto object_lsn = part->object->meta.lsn;
			if (object_lsn < lsn)
				lsn = object_lsn;
		}

		unlock(lock_mgr, &table->rel, LOCK_SHARED);
	}

	unlock_catalog(lock_mgr, LOCK_SHARED);
	unlock_checkpoint(lock_mgr, LOCK_EXCLUSIVE);

	// remove wal files < lsn
	wal_gc(&self->wal_mgr.wal, lsn);
}
