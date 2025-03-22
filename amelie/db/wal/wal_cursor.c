
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>

void
wal_cursor_init(WalCursor* self)
{
	self->file_offset = 0;
	self->file        = NULL;
	self->file_next   = true;
	self->crc         = false;
	self->wal         = NULL;
	buf_init(&self->buf);
}

void
wal_cursor_open(WalCursor* self, Wal* wal, uint64_t lsn,
                bool       file_next,
                bool       crc)
{
	self->file        = NULL;
	self->file_offset = 0;
	self->file_next   = file_next;
	self->crc         = crc;
	self->wal         = wal;

	// find nearest file with id <= lsn
	uint64_t id;
	if (lsn == 0)
		id = id_mgr_min(&wal->list);
	else
		id = id_mgr_find(&wal->list, lsn);
	if (id == UINT64_MAX)
		return;
	self->file = wal_file_allocate(id);
	wal_file_open(self->file);

	// rewind to the start lsn
	for (;;)
	{
		if (! wal_cursor_next(self))
			break;
		auto record = wal_cursor_at(self);
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
	buf_free(&self->buf);
	wal_cursor_init(self);
}

bool
wal_cursor_active(WalCursor* self)
{
	return self->file != NULL;
}

Record*
wal_cursor_at(WalCursor* self)
{
	return (Record*)self->buf.start;
}

hot static Record*
wal_cursor_read(WalCursor* self)
{
	if (unlikely(self->file == NULL))
		return NULL;

	auto wal = self->wal;
	for (;;)
	{
		auto buf_offset = buf_size(&self->buf);
		auto file = self->file;
		if (wal_file_pread(file, self->file_offset, &self->buf))
		{
			// validate crc (header + commands)
			auto record = (Record*)(self->buf.start + buf_offset);
			if (self->crc)
			{
				if (unlikely(! record_validate(record)))
					error("wal: record crc mismatch");
			}
			self->file_offset += record->size;
			return record;
		}

		// retry read if file size has changed
		if (file_update_size(&file->file))
			continue;

		if (! self->file_next)
			break;

		// get to the next file id
		uint64_t id;
		id = id_mgr_next(&wal->list, file->id);
		if (id == UINT64_MAX)
			break;

		// close previous file
		wal_file_close(file);
		wal_file_free(file);

		// open next file
		self->file_offset = 0;
		self->file        = wal_file_allocate(id);
		wal_file_open(self->file);
	}

	return NULL;
}

bool
wal_cursor_next(WalCursor* self)
{
	buf_reset(&self->buf);
	return wal_cursor_read(self) != NULL;
}

int
wal_cursor_readahead(WalCursor* self, int size,
                     uint64_t*  lsn,
                     uint64_t   lsn_max)
{
	buf_reset(&self->buf);
	int collected = 0;
	while (collected < size)
	{
		auto record = wal_cursor_read(self);
		if (! record)
			break;
		*lsn = record->lsn;
		collected += record->size;
		if (record->lsn == lsn_max)
			break;
	}
	return collected;
}
