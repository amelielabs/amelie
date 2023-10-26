
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_wal.h>

void
wal_cursor_init(WalCursor* self)
{
	self->file_offset = 0;
	self->file        = NULL;
	self->wal         = NULL;
}

void
wal_cursor_open(WalCursor* self, Wal* wal, uint64_t lsn_start)
{
	self->file        = NULL;
	self->file_offset = 0;
	self->wal         = wal;

	// find nearest file with id <= lsn_start
	uint64_t id;
	if (lsn_start == 0)
		id = snapshot_mgr_min(&wal->wal_store.list);
	else
		id = snapshot_mgr_find(&wal->wal_store.list, lsn_start);
	if (id == UINT64_MAX)
		return;
	self->file = wal_file_allocate(id);
	wal_file_open(self->file);

	// rewind to the start lsn
	for (;;)
	{
		auto buf = wal_cursor_next(self);
		if (unlikely(buf == NULL))
			break;
		// [lsn, [log]]
		auto msg = msg_of(buf);
		uint8_t* pos = msg->data;
		int      count;
		int64_t  lsn;
		data_read_array(&pos, &count);
		data_read_integer(&pos, &lsn);
		buf_free(buf);
		if (lsn >= (int64_t)lsn_start)
		{
			// rewind
			self->file_offset -= msg->size;
			break;
		}
	}
}

void
wal_cursor_close(WalCursor* self)
{
	if (self->file)
	{
		wal_file_close(self->file);
		wal_file_free(self->file);
		self->file = NULL;
	}
}

Buf*
wal_cursor_next(WalCursor* self)
{
	if (unlikely(self->file == NULL))
		return NULL;

	auto wal = self->wal;
	for (;;)
	{
		auto file = self->file;
		auto buf = wal_file_pread_buf(file, self->file_offset);
		if (buf)
		{
			self->file_offset += msg_of(buf)->size;
			return buf;
		}

		// retry read if size changed
		if (file_update_size(&file->file))
			continue;

		// get to the next file id
		uint64_t id;
		id = snapshot_mgr_next(&wal->wal_store.list, file->id);
		if (id == UINT64_MAX)
			break;

		// retry again to avoid race between last write
		if (file_update_size(&file->file))
			continue;

		// close previous file
		wal_file_close(file);
		wal_file_free(file);

		// open next file
		self->file_offset = 0;
		self->file        = NULL;
		self->file        = wal_file_allocate(id);
		wal_file_open(self->file);
	}

	return NULL;
}
