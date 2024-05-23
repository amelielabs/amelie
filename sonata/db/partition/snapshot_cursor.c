
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
snapshot_cursor_init(SnapshotCursor* self)
{
	self->file_offset = 0;
	file_init(&self->file);
}

void
snapshot_cursor_open(SnapshotCursor* self, uint64_t lsn, uint64_t storage)
{
	// <base>/<lsn>/<storage>
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%" PRIu64 "/%" PRIu64,
	         config_directory(),
	         lsn,
	         storage);
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

	auto buf = buf_begin();

	// read header
	uint32_t size_header = sizeof(Msg);
	file_pread_buf(&self->file, buf, size_header, self->file_offset);
	self->file_offset += size_header;

	uint32_t size = msg_of(buf)->size;
	uint32_t size_data;
	size_data = size - size_header;

	// check for eof
	if (snapshot_cursor_eof(self, size_data))
	{
		buf_end(buf);
		buf_free(buf);
		return NULL;
	}

	// read body
	file_pread_buf(&self->file, buf, size_data, self->file_offset);
	self->file_offset += size_data;

	buf_end(buf);
	return buf;
}
