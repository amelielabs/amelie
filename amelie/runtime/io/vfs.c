
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

int
vfs_unlink(const char* path)
{
	return unlink(path);
}

int
vfs_rename(const char* src, const char* dest)
{
	return rename(src, dest);
}

int
vfs_mkdir(const char* path, int mode)
{
	return mkdir(path, mode);
}

int
vfs_rmdir(const char* path)
{
	return rmdir(path);
}

int
vfs_open(const char* path, int flags, int mode)
{
	return open(path, flags, mode);
}

int
vfs_close(int fd)
{
	return close(fd);
}

int
vfs_flock_shared(int fd)
{
	return flock(fd, LOCK_SH|LOCK_NB);
}

int
vfs_flock_exclusive(int fd)
{
	return flock(fd, LOCK_EX|LOCK_NB);
}

int
vfs_flock_unlock(int fd)
{
	return flock(fd, LOCK_UN|LOCK_NB);
}

int
vfs_fdatasync(int fd)
{
#if defined(__APPLE__)
	return fcntl(fd, F_FULLFSYNC);
#elif defined(__FreeBSD__) || defined(__DragonFly__)
	return fsync(fd);
#else
	return fdatasync(fd);
#endif
}

int
vfs_fsync(int fd)
{
#if defined(__APPLE__)
	return fcntl(fd, F_FULLFSYNC);
#else
	return fsync(fd);
#endif
}

int
vfs_sync_file_range(int fd, uint64_t off, uint64_t size)
{
	int rc;
#ifdef __linux__
	rc = sync_file_range(fd, off, size,
	                     SYNC_FILE_RANGE_WRITE|
	                     SYNC_FILE_RANGE_WAIT_AFTER);
#else
	rc = vfs_sync(f, fd);
	(void)off;
	(void)size;
#endif
	return rc;
}

int
vfs_truncate(int fd, uint64_t size)
{
	return ftruncate(fd, size);
}

int64_t
vfs_read_raw(int fd, void* buf, uint64_t size)
{
	int r;
	do {
		r = read(fd, (char*)buf, size);
	} while (r == -1 && errno == EINTR);

	return r;
}

int64_t
vfs_read(int fd, void* buf, uint64_t size)
{
	uint64_t n = 0;
	do {
		int r;
		do {
			r = read(fd, (char*)buf + n, size - n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

int64_t
vfs_pread(int fd, uint64_t off, void* buf, uint64_t size)
{
	uint64_t n = 0;
	do {
		int r;
		do {
			r = pread(fd, (char*)buf + n, size - n, off + n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

int64_t
vfs_write(int fd, void* buf, uint64_t size)
{
	uint64_t n = 0;
	do {
		int r;
		do {
			r = write(fd, (char*)buf + n, size - n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

int64_t
vfs_pwrite(int fd, void* buf, uint64_t size, uint64_t offset)
{
	uint64_t n = 0;
	do {
		int r;
		do {
			r = pwrite(fd, (char*)buf + n, size - n, offset + n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

int64_t
vfs_writev(int fd, struct iovec* v, int n)
{
	uint64_t size = 0;
	do {
		int r;
		do {
			r = writev(fd, v, n);
		} while (r == -1 && errno == EINTR);
		if (r < 0)
			return -1;
		size += r;
		while (n > 0)
		{
			if (v->iov_len > (size_t)r) {
				v->iov_base = (char*)v->iov_base + r;
				v->iov_len -= r;
				break;
			}
			r -= v->iov_len;
			v++;
			n--;
		}
	} while (n > 0);

	return size;
}

int64_t
vfs_pwritev(int fd, uint64_t offset, struct iovec* v, int n)
{
	uint64_t size = 0;
	do {
		int r;
		do {
			r = pwritev(fd, v, n, offset + size);
		} while (r == -1 && errno == EINTR);
		if (r < 0)
			return -1;
		size += r;
		while (n > 0) {
			if (v->iov_len > (size_t)r) {
				v->iov_base = (char*)v->iov_base + r;
				v->iov_len -= r;
				break;
			} else {
				r -= v->iov_len;
				v++;
				n--;
			}
		}
	} while (n > 0);

	return size;
}

int64_t
vfs_seek(int fd, uint64_t off)
{
	return lseek(fd, off, SEEK_SET);
}

int64_t
vfs_size_fd(int fd)
{
	struct stat st;
	int rc = fstat(fd, &st);
	if (unlikely(rc == -1))
		return -1;
	return st.st_size;
}

int64_t
vfs_size(char* path)
{
	struct stat st;
	int rc = lstat(path, &st);
	if (unlikely(rc == -1))
		return -1;
	return st.st_size;
}

void*
vfs_mmap(int fd, uint64_t size)
{
	int prot = PROT_READ|PROT_WRITE;
	void *pointer = mmap(NULL, size, prot, MAP_PRIVATE|MAP_ANONYMOUS, fd, 0);
	if (pointer == MAP_FAILED)
		return NULL;
	return pointer;
}

void*
vfs_mremap(void* src, uint64_t src_size, uint64_t size)
{
	void* pointer;
	pointer = mremap(src, src_size, size, MREMAP_MAYMOVE);
	if (unlikely(pointer == MAP_FAILED))
		return NULL;
	return pointer;
}

int
vfs_munmap(void* pointer, uint64_t size)
{
	if (unlikely(pointer == NULL))
		return 0;
	int rc = munmap(pointer, size);
	return rc;
}
