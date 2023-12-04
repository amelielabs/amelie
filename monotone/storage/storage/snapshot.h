#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Snapshot Snapshot;

struct Snapshot
{
	Iov         iov;
	Buf         data;
	SnapshotId* id;
};

static inline void
snapshot_init(Snapshot* self)
{
	self->id = NULL;
	iov_init(&self->iov);
	buf_init(&self->data);
}

static inline void
snapshot_free(Snapshot* self)
{
	iov_free(&self->iov);
	buf_free(&self->data);
}

static inline void
snapshot_reset(Snapshot* self)
{
	self->id = NULL;
	iov_reset(&self->iov);
	buf_reset(&self->data);
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
}

hot static inline void
snapshot_create(Snapshot* self, SnapshotId* id, Index* index)
{
	auto def = index_def(index);
	self->id = id;

	// prepare snapshot data
	int64_t count = index_count(index);
	buf_reserve(&self->data, sizeof(Msg) * count);
	iov_reserve(&self->iov, 2 * count);

	// dump index rows
	Iterator* it = index_open(index, NULL, true);
	guard(guard, iterator_close, it);
	for (;;)
	{
		auto row = iterator_at(it);
		if (unlikely(! row))
			break;
		snapshot_add(self, def, row);
	}
}

static inline void
snapshot_create_file(Snapshot* self)
{
	char path[PATH_MAX];
	File file;
	file_init(&file);

	Exception e;
	if (try(&e))
	{
		// create <base>/storage_uuid/ if not exists
		snapshot_id_path_storage(self->id, path);
		if (! fs_exists("%s", path))
			fs_mkdir(0755, "%s", path);

		// <base>/storage_uuid/min.max.lsn.incomplete
		snapshot_id_path(self->id, path, true);

		// create
		file_create(&file, path);

		snapshot_id_path_name(self->id, path, true);
		log("snapshot: begin %s/%s", self->id->uuid_sz, path);

		// write snapshot content
		file_writev(&file, iov_pointer(&self->iov), self->iov.iov_count);

		// sync
		file_sync(&file);

		// rename incomplete file to <base>/storage_uuid/min.max.lsn
		snapshot_id_path(self->id, path, false);
		file_rename(&file, path);

		snapshot_id_path_name(self->id, path, false);
		log("snapshot: complete %s/%s", self->id->uuid_sz, path);
	}

	file_close(&file);

	if (catch(&e))
	{
		snapshot_id_path(self->id, path, true);
		fs_unlink("%s", path);
		rethrow();
	}
}
