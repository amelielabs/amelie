
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
#include <monotone_snapshot.h>

void
snapshot_cursor_init(SnapshotCursor* self)
{
	self->file_offset = 0;
	file_init(&self->file);
}

void
snapshot_cursor_open(SnapshotCursor* self, Uuid* uuid, uint64_t lsn)
{
	// <base>/snapshot/storage_uuid.lsn
	char uuid_sz[UUID_SZ];
	uuid_to_string(uuid, uuid_sz, sizeof(uuid_sz));

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/snapshot/%s.%020" PRIu64,
	         config_directory(),
	         uuid_sz,
	         lsn);

	// open
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

	auto buf = buf_create(0);

	// read header
	uint32_t size_header = sizeof(Msg);
	int start = buf_size(buf);
	file_pread_buf(&self->file, buf, size_header, self->file_offset);
	self->file_offset += size_header;
	uint32_t size = ((Msg*)(buf->start + start))->size;

	// check for eof
	if (snapshot_cursor_eof(self, size))
	{
		buf_free(buf);
		return NULL;
	}

	// read body
	uint32_t size_data;
	size_data = size - size_header;
	file_pread_buf(&self->file, buf, size_data, self->file_offset);
	self->file_offset += size_data;
	return buf;
}
