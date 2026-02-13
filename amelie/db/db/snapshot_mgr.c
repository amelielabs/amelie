
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
#include <amelie_wal.h>
#include <amelie_db.h>

static inline Snapshot*
snapshot_allocate(void)
{
	auto self = (Snapshot*)am_malloc(sizeof(Snapshot));
	buf_init(&self->data);
	wal_slot_init(&self->wal_snapshot);
	list_init(&self->link);
	return self;
}

static inline void
snapshot_free(Snapshot* self)
{
	buf_free(&self->data);
	am_free(self);
}

void
snapshot_mgr_init(SnapshotMgr* self, Catalog* catalog, Wal* wal)
{
	self->catalog    = catalog;
	self->wal        = wal;
	self->list_count = 0;
	list_init(&self->list);
	list_init(&self->list_gc);
}

void
snapshot_mgr_free(SnapshotMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto snapshot = list_at(Snapshot, link);
		snapshot_free(snapshot);
	}
}

Snapshot*
snapshot_mgr_create(SnapshotMgr* self)
{
	// create and register snapshot
	auto snapshot = snapshot_allocate();
	list_append(&self->list, &snapshot->link);
	self->list_count++;

	auto on_error = error_catch
	(
		// {}
		auto data = &snapshot->data;
		encode_obj(data);

		// version
		encode_raw(data, "version", 7);
		encode_obj(data);
		encode_raw(data, "version", 7);
		encode_string(data, &state()->version.string);
		encode_obj_end(data);

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

		// create catalog dump and take partitions snapshots
		//
		// catalog, storage, partitions
		catalog_snapshot(self->catalog, data);

		// create wal list and take the wal snapshot
		//
		// wal
		encode_raw(data, "wal", 3);
		wal_snapshot(self->wal, &snapshot->wal_snapshot, data);

		encode_obj_end(data);
	);

	if (on_error)
	{
		snapshot_mgr_drop(self, snapshot);
		rethrow();
	}

	return snapshot;
}

static void
snapshot_mgr_gc(SnapshotMgr* self)
{
	list_foreach_safe(&self->list_gc)
	{
		auto snapshot = list_at(Snapshot, link);
		auto data = &snapshot->data;
		if (! buf_empty(data)) {
			error_catch ( catalog_snapshot_cleanup(data) );
		}
		snapshot_free(snapshot);
	}
	list_init(&self->list_gc);
}

void
snapshot_mgr_drop(SnapshotMgr* self, Snapshot* snapshot)
{
	// detach wal slot
	wal_del(self->wal, &snapshot->wal_snapshot);

	list_unlink(&snapshot->link);
	self->list_count--;

	// add to the delayed gc list
	list_append(&self->list_gc, &snapshot->link);

	// cleanup snapshot files
	if (self->list_count > 0)
		return;

	// do gc once there are no active snapshots left
	snapshot_mgr_gc(self);
}
