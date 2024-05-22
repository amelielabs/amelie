
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>

void
snapshot_init(Snapshot* self)
{
	self->count       = 0;
	self->count_batch = 10000;
	self->id          = NULL;
	file_init(&self->file);
	iov_init(&self->iov);
	buf_init(&self->data);
}

void
snapshot_free(Snapshot* self)
{
	iov_free(&self->iov);
	buf_free(&self->data);
}

void
snapshot_reset(Snapshot* self)
{
	self->count = 0;
	self->id    = NULL;
	file_init(&self->file);
	iov_reset(&self->iov);
	buf_reset(&self->data);
}

static bool
snapshot_begin(Snapshot* self)
{
	// <base>/<storage_id>.<lsn>.incomplete
	char path[PATH_MAX];
	snapshot_id_path(self->id, path, true);

	if (fs_exists("%s", path))
	{
		log("checkpoint %" PRIu64 ": %s already exists", self->id->lsn, path);
		return false;
	}

	log("checkpoint %" PRIu64 ": %s begin", self->id->lsn, path);

	// prepare batch
	buf_reserve(&self->data, sizeof(Msg) * self->count_batch);
	iov_reserve(&self->iov, 2 * self->count_batch);

	// create
	file_create(&self->file, path);
	return true;
}

static void
snapshot_end(Snapshot* self)
{
	// todo: sync?

	// rename incomplete file to <base>/<storage_id>.<lsn>
	char path[PATH_MAX];
	snapshot_id_path(self->id, path, false);

	file_rename(&self->file, path);
	file_close(&self->file);

	double size = self->file.size / 1024 / 1024;
	log("checkpoint %" PRIu64 ": %s complete (%.2f MiB)", self->id->lsn,
	    path, size);
}

static void
snapshot_abort(Snapshot* self)
{
	char path[PATH_MAX];
	snapshot_id_path(self->id, path, true);
	file_close(&self->file);
	fs_unlink("%s", path);
}

hot static inline void
snapshot_add(Snapshot* self, Def* def, Row* row)
{
	uint8_t* data = row_data(row, def);
	int      data_size = row_data_size(row, def);

	// MSG_SNAPSHOT_ROW
	auto msg = (Msg*)self->data.position;
	msg->id   = MSG_SNAPSHOT_ROW;
	msg->size = sizeof(Msg) + data_size;
	buf_advance(&self->data, sizeof(Msg));

	iov_add(&self->iov, msg, sizeof(Msg));
	iov_add(&self->iov, data, data_size);

	self->count++;
}

hot static void
snapshot_flush(Snapshot* self)
{
	file_writev(&self->file, iov_pointer(&self->iov), self->iov.iov_count);
	self->count = 0;
	iov_reset(&self->iov);
	buf_reset(&self->data);
}

hot static void
snapshot_main(Snapshot* self, Index* index)
{
	auto def = index_def(index);
	auto it = index_open(index, NULL, true);
	guard(iterator_close, it);
	
	for (;;)
	{
		auto row = iterator_at(it);
		if (unlikely(! row))
			break;
		snapshot_add(self, def, row);
		if (self->count == self->count_batch)
			snapshot_flush(self);
		iterator_next(it);
	}
	if (self->count > 0)
		snapshot_flush(self);
}

hot void
snapshot_create(Snapshot* self, SnapshotId* id, Index* index)
{
	self->id = id;

	Exception e;
	if (enter(&e))
	{
		// create <base>/<storage_id>.<lsn>.incomplete
		
		// do nothing, if file exists
		if (snapshot_begin(self))
		{
			// write index rows
			snapshot_main(self, index);

			// rename snapshot file as completed
			snapshot_end(self);
		}
	}

	if (leave(&e))
	{
		snapshot_abort(self);
		rethrow();
	}
}
