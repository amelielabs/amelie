
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>

void
snapshot_cursor_init(SnapshotCursor* self)
{
	self->file_offset = 0;
	file_init(&self->file);
}

void
snapshot_cursor_open(SnapshotCursor* self, uint64_t lsn, uint64_t partition)
{
	// <base>/checkpoints/<lsn>/<partition>.part
	char path[PATH_MAX];
	snprintf(path, sizeof(path),
	         "%s/checkpoints/%" PRIu64 "/%010" PRIu64 ".part",
	         config_directory(),
	         lsn,
	         partition);
	file_open(&self->file, path);
}

void
snapshot_cursor_close(SnapshotCursor* self)
{
	file_close(&self->file);
}

static inline bool
snapshot_cursor_eof(SnapshotCursor* self, uint32_t size)
{
	return (self->file_offset + size) > self->file.size;
}

Buf*
snapshot_cursor_next(SnapshotCursor* self)
{
	// check for eof
	if (snapshot_cursor_eof(self, sizeof(Msg)))
		return NULL;

	auto buf = buf_create();
	errdefer_buf(buf);

	// read header
	uint32_t size_header = sizeof(Msg);
	file_pread_buf(&self->file, buf, size_header, self->file_offset);
	self->file_offset += size_header;

	uint32_t size = msg_of(buf)->size;
	uint32_t size_data;
	size_data = size - size_header;

	// check for eof
	if (snapshot_cursor_eof(self, size_data)) {
		buf_free(buf);
		return NULL;
	}

	// read body
	file_pread_buf(&self->file, buf, size_data, self->file_offset);
	self->file_offset += size_data;
	return buf;
}
