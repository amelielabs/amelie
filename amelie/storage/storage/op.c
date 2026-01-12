
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
	// create a list of all pending partitions
	auto list = buf_create();
	errdefer_buf(list);

	// todo: get lsn and create checkpoint file

	auto lock_mgr = &self->lock_mgr;
	lock_checkpoint(lock_mgr, LOCK_EXCLUSIVE);
	lock_catalog(lock_mgr, LOCK_SHARED);

	list_foreach(&self->catalog.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		lock(lock_mgr, &table->rel, LOCK_SHARED);

		list_foreach(&table->volume_mgr.parts)
		{
			auto part = list_at(Part, link);
			// todo: only if has updates
			buf_write(list, &part->id, sizeof(part->id));
		}

		unlock(lock_mgr, &table->rel, LOCK_SHARED);
	}

	unlock_catalog(lock_mgr, LOCK_SHARED);
	unlock_checkpoint(lock_mgr, LOCK_EXCLUSIVE);
	return list;
}

void
storage_checkpoint(Storage* self)
{
	// create a list of all pending partitions
	auto list = storage_pending(self);
	defer_buf(list);

	// refresh partitions
	Refresh refresh;
	refresh_init(&refresh, self);
	defer(refresh_free, &refresh);

	auto end = (Id*)list->position;
	auto it  = (Id*)list->start;
	for (; it < end; it++)
		storage_refresh(self, &refresh, it, NULL);
}
