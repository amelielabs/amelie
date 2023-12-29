#pragma once

//
// indigo
//
// SQL OLTP database
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
	WalFile* self = in_malloc(sizeof(WalFile));
	self->id = id;
	file_init(&self->file);
	return self;
}

static inline void
wal_file_free(WalFile* self)
{
	in_free(self);
}

static inline void
wal_file_open(WalFile* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wal/%020" PRIu64 ".wal",
	         config_directory(),
	         self->id);
	file_open(&self->file, path);
}

static inline void
wal_file_create(WalFile* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wal/%020" PRIu64 ".wal",
	         config_directory(),
	         self->id);
	file_create(&self->file, path);
}

static inline void
wal_file_close(WalFile* self)
{
	file_close(&self->file);
}

static inline void
wal_file_write(WalFile* self, struct iovec* iov, int iovc)
{
	file_writev(&self->file, iov, iovc);
	if (var_int_of(&config()->wal_sync_on_write))
		file_sync(&self->file);
}

static inline bool
wal_file_eof(WalFile* self, uint32_t offset, uint32_t size)
{
	return (offset + size) > self->file.size;
}

static inline bool
wal_file_pread(WalFile* self, uint64_t offset, Buf* buf)
{
	// check for eof
	if (wal_file_eof(self, offset, sizeof(Msg)))
		return true;

	// read header
	uint32_t size_header = sizeof(Msg);
	int start = buf_size(buf);
	file_pread_buf(&self->file, buf, size_header, offset);
	uint32_t size = ((Msg*)(buf->start + start))->size;

	// check for eof
	if (wal_file_eof(self, offset, size))
		return true;

	// read body
	uint32_t size_data;
	size_data = size - size_header;
	file_pread_buf(&self->file, buf, size_data, offset + size_header);
	return false;
}

static inline Buf*
wal_file_pread_buf(WalFile* self, uint64_t offset)
{
	auto buf = buf_create(0);
	bool eof = wal_file_pread(self, offset, buf);
	if (! eof)
		return buf;
	buf_free(buf);
	return NULL;
}
