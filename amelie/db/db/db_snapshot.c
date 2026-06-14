
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
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>
#include <amelie_db.h>

static inline DbSnapshot*
db_snapshot_allocate(void)
{
	auto self = (DbSnapshot*)am_malloc(sizeof(DbSnapshot));
	self->checkpoint = NULL;
	buf_init(&self->data);
	wal_slot_init(&self->wal_snapshot);
	list_init(&self->link);
	return self;
}

static inline void
db_snapshot_free(DbSnapshot* self)
{
	buf_free(&self->data);
	am_free(self);
}

DbSnapshot*
db_snapshot(Db* self)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	defer(unlock, lock_catalog);

	// create and register snapshot
	auto snapshot = db_snapshot_allocate();
	list_append(&self->snapshots, &snapshot->link);
	self->snapshots_count++;

	auto on_error = error_catch
	(
		// {}
		auto data = &snapshot->data;
		encode_obj(data);

		// version
		encode_raw(data, "version", 7);
		encode_obj(data);
		encode_raw(data, "version", 7);
		encode_str(data, &state()->version.string);
		encode_obj_end(data);

		// server
		encode_raw(data, "server", 6);
		buf_write_str(data, &config()->listen.string);

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

		// use last checkpoint
		snapshot->checkpoint = checkpoint_mgr_ref(&self->checkpoint_mgr);

		// checkpoint
		encode_raw(data, "checkpoint", 10);
		encode_int(data, snapshot->checkpoint->id);

		// files
		encode_raw(data, "files", 5);
		checkpoint_mgr_list(snapshot->checkpoint, data);

		// create wal list and take the wal snapshot

		// wal
		encode_raw(data, "wal", 3);
		wal_snapshot(&self->wal, &snapshot->wal_snapshot, data);

		encode_obj_end(data);
	);

	if (on_error)
	{
		db_snapshot_drop(self, snapshot);
		rethrow();
	}

	return snapshot;
}

static void
db_snapshot_detach(Db* self, DbSnapshot* snapshot)
{
	list_unlink(&snapshot->link);
	self->snapshots_count--;

	// detach wal slot
	wal_detach(&self->wal, &snapshot->wal_snapshot);

	// detach checkpoint
	checkpoint_mgr_unref(&self->checkpoint_mgr, snapshot->checkpoint);

	db_snapshot_free(snapshot);
}

void
db_snapshot_drop(Db* self, DbSnapshot* snapshot)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	error_catch (
		db_snapshot_detach(self, snapshot);
	);
	unlock(lock_catalog);

	// wal gc
	db_gc(self);
}
