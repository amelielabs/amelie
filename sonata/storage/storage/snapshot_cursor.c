
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>

void
snapshot_cursor_init(SnapshotCursor* self)
{
	self->file_offset = 0;
	file_init(&self->file);
}

void
snapshot_cursor_open(SnapshotCursor* self, SnapshotId* id)
{
	// <base>/storage_uuid/min.max.lsn
	char path[PATH_MAX];
	snapshot_id_path(id, path, false);

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
