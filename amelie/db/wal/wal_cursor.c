
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
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
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
	self->wal         = NULL;
	buf_init(&self->buf);
}

void
wal_cursor_open(WalCursor* self, Wal* wal, uint64_t lsn)
{
	self->file        = NULL;
	self->file_offset = 0;
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
		auto write = wal_cursor_at(self);
		if (write->lsn >= lsn)
		{
			// rewind
			self->file_offset -= write->size;
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

WalWrite*
wal_cursor_at(WalCursor* self)
{
	return (WalWrite*)self->buf.start;
}

hot static WalWrite*
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
			auto write = (WalWrite*)(self->buf.start + buf_offset);
			self->file_offset += write->size;
			return write;
		}

		// retry read if file size has changed
		if (file_update_size(&file->file))
			continue;

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
		self->file        = NULL;
		self->file        = wal_file_allocate(id);
		wal_file_open(self->file);
	}

	return NULL;
}

bool
wal_cursor_next(WalCursor* self)
{
	buf_reset(&self->buf);
	auto write = wal_cursor_read(self);
	if (write)
		return true;
	return false;
}

bool
wal_cursor_collect(WalCursor* self, int size, uint64_t* lsn)
{
	buf_reset(&self->buf);
	int collected = 0;
	while (collected < size)
	{
		auto write = wal_cursor_read(self);
		if (! write)
			break;
		*lsn = write->lsn;
		collected += write->size;
	}
	return collected > 0;
}
