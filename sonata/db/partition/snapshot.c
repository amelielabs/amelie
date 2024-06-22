
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>

void
snapshot_init(Snapshot* self)
{
	self->count       = 0;
	self->count_batch = 10000;
	self->partition   = 0;
	self->lsn         = 0;
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
	self->count     = 0;
	self->partition = 0;
	self->lsn       = 0;
	file_init(&self->file);
	iov_reset(&self->iov);
	buf_reset(&self->data);
}

static void
snapshot_begin(Snapshot* self)
{
	// <base>/<lsn>.incomplete/<partition_id>
	char path[PATH_MAX];
	snprintf(path, sizeof(path),
	         "%s/%" PRIu64 ".incomplete/%" PRIu64,
	         config_directory(),
	         self->lsn,
	         self->partition);

	log("snapshot: %s begin", path);

	// prepare batch
	buf_reserve(&self->data, sizeof(Msg) * self->count_batch);
	iov_reserve(&self->iov, 2 * self->count_batch);

	// create
	file_create(&self->file, path);
}

static void
snapshot_end(Snapshot* self)
{
	// todo: sync?
	file_close(&self->file);

	char path[PATH_MAX];
	snprintf(path, sizeof(path),
	         "%s/%" PRIu64 ".incomplete/%" PRIu64,
	         config_directory(),
	         self->lsn,
	         self->partition);

	double size = self->file.size / 1024 / 1024;
	log("snapshot: %s complete (%.2f MiB)", path, size);
}

hot static inline void
snapshot_add(Snapshot* self, Row* row)
{
	uint8_t* data = row_data(row);
	int      data_size = row_size(row);

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
	auto it = index_open(index, NULL, true);
	guard(iterator_close, it);
	
	while (iterator_has(it))
	{
		auto row = iterator_at(it);
		snapshot_add(self, row);
		if (self->count == self->count_batch)
			snapshot_flush(self);
		iterator_next(it);
	}
	if (self->count > 0)
		snapshot_flush(self);
}

hot void
snapshot_create(Snapshot* self, Part* part, uint64_t lsn)
{
	self->partition = part->config->id;
	self->lsn       = lsn;

	Exception e;
	if (enter(&e))
	{
		// create <base>/<lsn>.incomplete/<partition_id>
		snapshot_begin(self);
		snapshot_main(self, part_primary(part));
		snapshot_end(self);
	}
	if (leave(&e))
	{
		file_close(&self->file);
		rethrow();
	}
}
