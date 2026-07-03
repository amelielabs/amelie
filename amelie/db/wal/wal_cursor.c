
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>

void
wal_cursor_init(WalCursor* self)
{
	self->file_offset = 0;
	self->file        = NULL;
	self->file_next   = true;
	self->crc         = false;
	self->follow      = false;
	self->wal         = NULL;
}

void
wal_cursor_open(WalCursor* self, Wal* wal, uint64_t lsn,
                bool       file_next,
                bool       crc,
                bool       follow)
{
	self->file        = NULL;
	self->file_offset = 0;
	self->file_next   = file_next;
	self->crc         = crc;
	self->follow      = follow;
	self->wal         = wal;

	// find nearest file with id <= lsn
	auto ref = wal_find(wal, lsn, false);
	if (! ref)
		return;
	defer(wal_file_unpin_defer, ref);

	// open wal file separately
	self->file = wal_file_allocate(ref->id);
	wal_file_open(self->file);

	// rewind to the start lsn
	for (;;)
	{
		auto buf = wal_cursor_next(self);
		if (! buf)
			break;
		defer_buf(buf);
		auto record = (Record*)buf->start;
		if (record->lsn >= lsn)
		{
			// rewind
			self->file_offset -= record->size;
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
	wal_cursor_init(self);
}

bool
wal_cursor_active(WalCursor* self)
{
	return self->file != NULL;
}

hot static bool
wal_cursor_read(WalCursor* self, Buf* buf)
{
	if (unlikely(self->file == NULL))
		return false;

	auto wal = self->wal;
	for (;;)
	{
		auto buf_offset = buf_size(buf);
		auto file = self->file;
		if (wal_file_pread(file, self->file_offset, buf))
		{
			// validate crc (header + commands)
			auto record = (Record*)(buf->start + buf_offset);
			if (self->crc)
			{
				if (unlikely(! record_validate(record)))
					error("wal: record crc mismatch");
			}
			self->file_offset += record->size;
			return true;
		}

		if (!self->follow && self->file_offset != file->file.size)
			error("wal: record is incomplete");

		// retry read if file size has changed
		if (file_update_size(&file->file))
			continue;

		if (! self->file_next)
			break;

		// get to the next file id
		auto ref = wal_find(wal, file->id, true);
		if (! ref)
			break;
		auto id = ref->id;
		wal_file_unpin(ref);

		// close previous file
		wal_file_close(file);
		wal_file_free(file);

		// open next file
		self->file_offset = 0;
		self->file        = wal_file_allocate(id);
		wal_file_open(file);
	}

	return false;
}

RecordMsg*
wal_cursor_next_msg(WalCursor* self)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	buf_emplace(buf, sizeof(RecordMsg));
	if (wal_cursor_read(self, buf))
	{
		auto msg = (RecordMsg*)buf->start;
		msg_init(&msg->msg, MSG_RECORD);
		msg->msg_buf     = buf;
		msg->arg         = NULL;
		msg->instance_id = *opt_uuid_of(&config()->uuid);
		return (RecordMsg*)buf->start;
	}

	buf_free(buf);
	return NULL;
}

Buf*
wal_cursor_next(WalCursor* self)
{
	auto buf = buf_create();
	errdefer_buf(buf);
	if (wal_cursor_read(self, buf))
		return buf;
	buf_free(buf);
	return NULL;
}

Buf*
wal_cursor_readahead(WalCursor* self, int size,
                     uint64_t*  lsn,
                     uint64_t   lsn_max)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	int collected = 0;
	while (collected < size)
	{
		auto offset = buf_size(buf);
		if (! wal_cursor_read(self, buf))
			break;
		auto record = (Record*)(buf->start + offset);
		*lsn = record->lsn;
		collected += record->size;
		if (record->lsn == lsn_max)
			break;
	}

	if (buf_empty(buf))
	{
		buf_free(buf);
		buf = NULL;
	}

	return buf;
}
