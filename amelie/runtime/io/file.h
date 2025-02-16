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

typedef struct File File;

struct File
{
	int      fd;
	uint64_t size;
	Str      path;
	List     link;
};

static inline void
file_init(File* self)
{
	self->fd   = -1;
	self->size = 0;
	str_init(&self->path);
}

static inline void
file_error(File* self, const char* operation)
{
	error("file: '%s' %s error: %s", str_of(&self->path), operation,
	      strerror(errno));
}

static inline void
file_open_stdin(File* self)
{
	// open file
	auto path = "/dev/stdin";
	str_dup_cstr(&self->path, path);
	self->size = 0;

	// open
	self->fd = vfs_open(str_of(&self->path), O_RDONLY, 0);
	if (unlikely(self->fd == -1))
		file_error(self, "open");
}

static inline void
file_open_as(File* self, const char* path, int flags, int mode)
{
	// open or create file
	str_dup_cstr(&self->path, path);

	// get file size
	if (! (flags & O_CREAT))
	{
		int64_t size = vfs_size(str_of(&self->path));
		if (unlikely(size == -1))
			file_error(self, "stat");
		self->size = size;
	}

	// open
	self->fd = vfs_open(str_of(&self->path), flags, mode);
	if (unlikely(self->fd == -1))
		file_error(self, "open");
}

static inline void
file_open(File* self, const char* path)
{
	// open existing file
	file_open_as(self, path, O_RDWR, 0);
}

static inline void
file_open_rdonly(File* self, const char* path)
{
	// open existing file
	file_open_as(self, path, O_RDONLY, 0);
}

static inline void
file_create(File* self, const char* path)
{
	// create file
	file_open_as(self, path, O_CREAT|O_RDWR|O_EXCL, 0644);
}

static inline void
file_close(File* self)
{
	if (self->fd != -1) {
		vfs_close(self->fd);
		self->fd = -1;
	}
	str_free(&self->path);
}

static inline void
file_seek_to_end(File* self)
{
	int rc;
	rc = vfs_seek(self->fd, self->size);
	if (unlikely(rc == -1))
		file_error(self, "seek");
}

static inline void
file_sync(File* self)
{
	int rc;
	rc = vfs_fdatasync(self->fd);
	if (unlikely(rc == -1))
		file_error(self, "sync");
}

static inline void
file_truncate(File* self, uint64_t size)
{
	int rc;
	rc = vfs_truncate(self->fd, size);
	if (unlikely(rc == -1))
		file_error(self, "truncate");
	self->size = size;
	file_seek_to_end(self);
}

static inline bool
file_update_size(File* self)
{
	int64_t size = vfs_size(str_of(&self->path));
	if (unlikely(size == -1))
		file_error(self, "stat");
	bool updated = self->size != (uint64_t)size;
	self->size = size;
	return updated;
}

static inline void
file_rename(File* self, const char* path)
{
	Str new_path;
	str_dup_cstr(&new_path, path);
	int rc = vfs_rename(str_of(&self->path), str_of(&new_path));
	if (unlikely(rc == -1))
	{
		str_free(&new_path);
		file_error(self, "rename");
	}
	str_free(&self->path);
	self->path = new_path;
}

static inline uint64_t
file_write(File* self, void* data, int size)
{
	uint64_t offset = self->size;
	int rc = vfs_write(self->fd, data, size);
	if (unlikely(rc == -1))
		file_error(self, "write");
	self->size += rc;
	return offset;
}

static inline uint64_t
file_write_buf(File* self, Buf* buf)
{
	return file_write(self, buf->start, buf_size(buf));
}

static inline void
file_writev(File* self, struct iovec* iov, int iov_count)
{
	int count_left = iov_count;
	while (count_left > 0)
	{
		int n;
		if (count_left > IOV_MAX)
			n = IOV_MAX;
		else
			n = count_left;
		int64_t rc = vfs_writev(self->fd, iov, n);
		if (unlikely(rc == -1))
			file_error(self, "writev");
		count_left -= n;
		iov += n;
		self->size += rc;
	}
}

static inline void
file_pwrite(File* self, void *data, int size, uint64_t offset)
{
	int rc = vfs_pwrite(self->fd, data, size, offset);
	if (unlikely(rc == -1))
		file_error(self, "pwrite");
}

static inline void
file_pwrite_buf(File* self, Buf* buf, uint64_t offset)
{
	file_pwrite(self, buf->start, buf_size(buf), offset);
}

static inline int64_t
file_read_raw(File* self, void* data, int size)
{
	int64_t rc = vfs_read_raw(self->fd, data, size);
	if (unlikely(rc == -1))
		file_error(self, "read");
	return rc;
}

static inline void
file_read(File* self, void* data, int size)
{
	int rc = vfs_read(self->fd, data, size);
	if (unlikely(rc == -1))
		file_error(self, "read");
}

static inline void
file_read_buf(File* self, Buf* buf, int size)
{
	buf_reserve(buf, size);
	file_read(self, buf->position, size);
	buf_advance(buf, size);
}

static inline void
file_pread(File* self, void* data, int size, uint64_t offset)
{
	int rc;
	rc = vfs_pread(self->fd, offset, data, size);
	if (unlikely(rc == -1))
		file_error(self, "pread");
}

static inline void
file_pread_buf(File* self, Buf* buf, int size, uint64_t offset)
{
	buf_reserve(buf, size);
	file_pread(self, buf->position, size, offset);
	buf_advance(buf, size);
}

static inline Buf*
file_import(const char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_rdonly(&file, path);
	file_pread_buf(&file, buf, file.size, 0);
	return buf;
}

static inline void
file_import_stream(Buf* buf, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_rdonly(&file, path);
	for (;;)
	{
		buf_reserve(buf, 16 * 1024);
		auto rc = file_read_raw(&file, buf->position, 16 * 1024);
		if (rc == 0)
			break;
		buf_advance(buf, rc);
	}
}
