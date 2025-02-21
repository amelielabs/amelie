#pragma once

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

typedef struct WalFile WalFile;

struct WalFile
{
	uint64_t id;
	File     file;
};

static inline WalFile*
wal_file_allocate(uint64_t id)
{
	WalFile* self = am_malloc(sizeof(WalFile));
	self->id = id;
	file_init(&self->file);
	return self;
}

static inline void
wal_file_free(WalFile* self)
{
	am_free(self);
}

static inline void
wal_file_open(WalFile* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wals/%" PRIu64,
	         state_directory(),
	         self->id);
	file_open(&self->file, path);
}

static inline void
wal_file_create(WalFile* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wals/%" PRIu64,
	         state_directory(),
	         self->id);
	file_create(&self->file, path);
}

static inline void
wal_file_close(WalFile* self)
{
	file_close(&self->file);
}

static inline void
wal_file_sync(WalFile* self)
{
	file_sync(&self->file);
}

static inline void
wal_file_write(WalFile* self, Write* write)
{
	// [header][commands][rows or ops]
	file_writev(&self->file, iov_pointer(&write->iov), write->iov.iov_count);
	list_foreach(&write->list)
	{
		auto log_write = list_at(LogWrite, link);
		file_writev(&self->file, iov_pointer(&log_write->iov),
		            log_write->iov.iov_count);
	}
}

static inline bool
wal_file_eof(WalFile* self, uint32_t offset, uint32_t size)
{
	return (offset + size) > self->file.size;
}

static inline bool
wal_file_pread(WalFile* self, uint64_t offset, bool crc, Buf* buf)
{
	// check for eof
	if (wal_file_eof(self, offset, sizeof(Record)))
		return false;

	// read header
	uint32_t size_header = sizeof(Record);
	int start = buf_size(buf);
	file_pread_buf(&self->file, buf, size_header, offset);
	uint32_t size = ((Record*)(buf->start + start))->size;

	// check for eof
	if (wal_file_eof(self, offset, size))
	{
		buf_truncate(buf, size_header);
		return false;
	}

	// read body
	uint32_t size_data;
	size_data = size - size_header;
	file_pread_buf(&self->file, buf, size_data, offset + size_header);

	// validate crc (header + commands)
	if (crc)
	{
		auto record = (Record*)(buf->start + start);
		if (unlikely(! record_validate(record)))
			error("wal: lsn %" PRIu64 ": crc mismatch", state_lsn() + 1);
	}

	return true;
}
