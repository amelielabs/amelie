
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_wal.h>

void
wal_store_init(WalStore* self)
{
	self->current = NULL;
	snapshot_mgr_init(&self->list);
	snapshot_mgr_init(&self->snapshot);
	wal_event_mgr_init(&self->event_mgr);
}

void
wal_store_free(WalStore* self)
{
	if (self->current)
	{
		auto file = self->current;
		wal_file_close(file);
		wal_file_free(file);
		self->current = NULL;
	}
	snapshot_mgr_free(&self->list);
	snapshot_mgr_free(&self->snapshot);
	wal_event_mgr_free(&self->event_mgr);
}

static inline bool
wal_store_rotate_ready(WalStore* self)
{
	uint64_t wal_rotate_wm = var_int_of(&config()->wal_rotate_wm);
	bool rotate;
	rotate = self->current == NULL || self->current->file.size > wal_rotate_wm;
	return rotate;
}

void
wal_store_rotate(WalStore* self, uint64_t next_lsn)
{
	// create new wal file
	auto file_prev = self->current;
	auto file = wal_file_allocate(next_lsn);

	Exception e;
	if (try(&e))
		wal_file_create(file);

	if (catch(&e))
	{
		wal_file_close(file);
		wal_file_free(file);
		rethrow();
	}

	// add to the list and set as current
	self->current = file;
	snapshot_mgr_add(&self->list, next_lsn);

	// sync and close prev file
	if (file_prev)
	{
		if (var_int_of(&config()->wal_sync_on_rotate))
			file_sync(&file_prev->file);
		wal_file_close(file_prev);
		wal_file_free(file_prev);
	}
}

void
wal_store_gc(WalStore* self, uint64_t snapshot)
{
	uint64_t snapshot_min = snapshot_mgr_min(&self->snapshot);
	uint64_t min = snapshot;
	if (snapshot_min < min)
		min = snapshot_min;

	Buf list;
	buf_init(&list);
	guard(list_guard, buf_free, &list);

	int list_count;
	list_count = snapshot_mgr_gc_between(&self->list, &list, min);

	// remove files by id
	if (list_count > 0)
	{
		char path[PATH_MAX];
		uint64_t *ids = (uint64_t*)list.start;
		int i = 0;
		for (; i < list_count; i++)
		{
			snprintf(path, sizeof(path), "%s/wal/%020" PRIu64 ".wal",
			         config_directory(),
			         ids[i]);
			log("gc: remove file %s", path);
			fs_unlink("%s", path);
		}
	}
}

void
wal_store_snapshot(WalStore* self, WalSnapshot* snapshot)
{
	// set snapshot to a first file id
	snapshot->snapshot     = snapshot_mgr_min(&self->list);
	snapshot->snapshot_mgr = &self->snapshot;
	snapshot_mgr_add(&self->snapshot, snapshot->snapshot);

	// copy file list
	snapshot->list_count = snapshot_mgr_copy(&self->list, &snapshot->list, UINT64_MAX);

	// set position of the last file and lsn
	snapshot->lsn       = config_lsn();
	snapshot->last_id   = self->current->id;
	snapshot->last_size = self->current->file.size;
}

hot void
wal_store_write(WalStore* self, LogSet* set)
{
	// maybe create a new wal file
	uint64_t lsn = config_lsn() + 1;

	if (wal_store_rotate_ready(self))
		wal_store_rotate(self, lsn);

	// create message
	log_set_create(set, lsn);

	// write file and in-memory index
//	wal_file_write(self->current, iov_pointer(&set->iov), set->iov.iov_count);

	// copy prepared iovec
	list_foreach(&set->list)
	{
		auto log = list_at(Log, link);

		wal_file_write(self->current, iov_pointer(&log->data_iov), log->data_iov.iov_count);
	}

	// update wal lsn
	config_lsn_set(lsn);

	// notify wal subscription
	wal_event_mgr_signal(&self->event_mgr, lsn);
}

Buf*
wal_store_status(WalStore* self)
{
	// map
	auto buf = msg_create(MSG_OBJECT);
	encode_map(buf, 5);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, config_lsn());

	int      list_count;
	uint64_t list_min;
	snapshot_mgr_stats(&self->list, &list_count, &list_min);

	// file_count
	encode_raw(buf, "file_count", 10);
	encode_integer(buf, list_count);

	// file_min
	encode_raw(buf, "file_min", 8);
	encode_integer(buf, list_min);

	int      snapshot_count;
	uint64_t snapshot_min;
	snapshot_mgr_stats(&self->snapshot, &snapshot_count, &snapshot_min);

	// snapshot_count
	encode_raw(buf, "snapshot_count", 14);
	encode_integer(buf, snapshot_count);

	// snapshot_min
	encode_raw(buf, "snapshot_min", 12);
	encode_integer(buf, snapshot_min);

	msg_end(buf);
	return buf;
}
