
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
#include <monotone_index.h>
#include <monotone_storage.h>

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

static void
snapshot_begin(Snapshot* self)
{
	// prepare batch
	buf_reserve(&self->data, sizeof(Msg) * self->count_batch);
	iov_reserve(&self->iov, 2 * self->count_batch);

	// create <base>/storage_uuid/ if not exists
	char path[PATH_MAX];
	snapshot_id_path_storage(self->id, path);
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// <base>/storage_uuid/min.max.lsn.incomplete
	snapshot_id_path(self->id, path, true);

	// create
	file_create(&self->file, path);
}

static void
snapshot_end(Snapshot* self)
{
	// sync
	/*file_sync(&self->file);*/

	// rename incomplete file to <base>/storage_uuid/min.max.lsn
	char path[PATH_MAX];
	snapshot_id_path(self->id, path, false);
	file_rename(&self->file, path);
	file_close(&self->file);
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
	// MSG_SNAPSHOT_ROW
	auto msg = (Msg*)self->data.position;
	buf_advance(&self->data, sizeof(Msg));
	msg->size = 0;
	msg->id   = MSG_SNAPSHOT_ROW;
	iov_add(&self->iov, msg, sizeof(Msg));

	// row data
	uint8_t* data = row_data(row, def);
	int      data_size = row_data_size(row, def);
	iov_add(&self->iov, data, data_size);
	msg->size = sizeof(Msg) + data_size;

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

	Iterator* it = index_open(index, NULL, true);
	guard(guard, iterator_close, it);

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
	if (try(&e))
	{
		// <base>/storage_uuid/min.max.lsn.incomplete
		snapshot_begin(self);

		// write index rows
		snapshot_main(self, index);

		// rename snapshot file as completed
		snapshot_end(self);
	}

	if (catch(&e))
	{
		snapshot_abort(self);
		rethrow();
	}
}
