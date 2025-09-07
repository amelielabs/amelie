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

hot static inline struct io_uring_sqe*
io_prepare(IoEvent* self)
{
	io_event_init(self);
	auto sqe = io_uring_get_sqe(am_task->io.ring);
	if (unlikely(! sqe))
		error("failed to allocate sqe");
	sqe->user_data = (uintptr_t)self;
	return sqe;
}

hot static inline ssize_t
io_wait(IoEvent* self)
{
	// schedule submit
	auto coro = am_self();
	io_set_pending(&am_task->io);

	// wait for completion
	wait_event(&self->event, coro);
	if (unlikely(coroutine_cancelled(coro)))
	{
		// cancel
		self->event.signal = false;
		coro->cancel = false;
		coro->cancel_pause = 0;
		coro->cancel_pause_recv = 0;

		coroutine_cancel_pause(coro);

		// process the cancelation request
		IoEvent cancel;
		io_uring_prep_cancel(io_prepare(&cancel), self, IORING_ASYNC_CANCEL_ALL);
		io_set_pending(&am_task->io);
		wait_event(&cancel.event, coro);

		// wait for the originall request completion
		wait_event(&self->event, coro);

		// cancellation point
		coroutine_cancel_resume(coro);
		rethrow();

		// unreach
	}

	auto rc = self->rc;
	if (unlikely(rc < 0))
	{
		errno = -rc;
		rc = -1;
	}
	return rc;
}

static inline ssize_t
io_open(const char* path, int flags, mode_t mode)
{
	IoEvent ev;
	io_uring_prep_openat(io_prepare(&ev), AT_FDCWD, path, flags, mode);
	return io_wait(&ev);
}

static inline ssize_t
io_close(int fd)
{
	IoEvent ev;
	io_uring_prep_close(io_prepare(&ev), fd);
	return io_wait(&ev);
}

static inline ssize_t
io_pread(int fd, void* data, size_t size, uint64_t offset)
{
	IoEvent ev;
	io_uring_prep_read(io_prepare(&ev), fd, data, size, offset);
	auto rc = io_wait(&ev);
	if (rc > 0)
		VALGRIND_MAKE_MEM_DEFINED(data, rc);
	return rc;
}

static inline ssize_t
io_preadv(int fd, const struct iovec* iov, int n, uint64_t offset)
{
	IoEvent ev;
	io_uring_prep_readv(io_prepare(&ev), fd, iov, n, offset);
	return io_wait(&ev);
}

static inline ssize_t
io_pwrite(int fd, void* data, size_t size, uint64_t offset)
{
	IoEvent ev;
	io_uring_prep_write(io_prepare(&ev), fd, data, size, offset);
	return io_wait(&ev);
}

static inline ssize_t
io_pwritev(int fd, const struct iovec* iov, int n, uint64_t offset)
{
	IoEvent ev;
	io_uring_prep_writev(io_prepare(&ev), fd, iov, n, offset);
	return io_wait(&ev);
}

static inline ssize_t
io_fsync(int fd)
{
	IoEvent ev;
	io_uring_prep_fsync(io_prepare(&ev), fd, 0);
	return io_wait(&ev);
}

static inline ssize_t
io_sync_file_range(int fd, size_t len, uint64_t offset, int flags)
{
	IoEvent ev;
	io_uring_prep_sync_file_range(io_prepare(&ev), fd, len, offset, flags);
	return io_wait(&ev);
}

static inline ssize_t
io_fallocate(int fd, int mode, uint64_t offset, uint64_t len)
{
	IoEvent ev;
	io_uring_prep_fallocate(io_prepare(&ev), fd, mode, offset, len);
	return io_wait(&ev);
}

static inline ssize_t
io_ftruncate(int fd, uint64_t len)
{
	IoEvent ev;
	io_uring_prep_ftruncate(io_prepare(&ev), fd, len);
	return io_wait(&ev);
}

static inline ssize_t
io_fadvise(int fd, uint64_t offset, off_t len, int advise)
{
	IoEvent ev;
	io_uring_prep_fadvise(io_prepare(&ev), fd, offset, len, advise);
	return io_wait(&ev);
}

static inline ssize_t
io_madvise(void* addr, off_t len, int advise)
{
	IoEvent ev;
	io_uring_prep_madvise(io_prepare(&ev), addr, len, advise);
	return io_wait(&ev);
}

static inline ssize_t
io_statx(const char* path, int flags, unsigned mask, struct statx* stat)
{
	IoEvent ev;
	io_uring_prep_statx(io_prepare(&ev), AT_FDCWD, path, flags, mask, stat);
	return io_wait(&ev);
}

static inline ssize_t
io_rename(const char* oldpath, const char* newpath)
{
	IoEvent ev;
	io_uring_prep_rename(io_prepare(&ev), oldpath, newpath);
	return io_wait(&ev);
}

static inline ssize_t
io_mkdir(const char* path, mode_t mode)
{
	IoEvent ev;
	io_uring_prep_mkdir(io_prepare(&ev), path, mode);
	return io_wait(&ev);
}

static inline ssize_t
io_unlink(const char* path, int flags)
{
	IoEvent ev;
	io_uring_prep_unlink(io_prepare(&ev), path, flags);
	return io_wait(&ev);
}

static inline ssize_t
io_socket(int domain, int type, int protocol, int flags)
{
	IoEvent ev;
	io_uring_prep_socket(io_prepare(&ev), domain, type, protocol, flags);
	return io_wait(&ev);
}

static inline ssize_t
io_connect(int fd, struct sockaddr* addr)
{
	socklen_t addrlen;
	if (addr->sa_family == AF_INET) {
		addrlen = sizeof(struct sockaddr_in);
	} else
	if (addr->sa_family == AF_INET6) {
		addrlen = sizeof(struct sockaddr_in6);
	} else
	if (addr->sa_family == AF_UNIX) {
		addrlen = sizeof(struct sockaddr_un);
	} else {
		errno = EINVAL;
		return -1;
	}
	IoEvent ev;
	io_uring_prep_connect(io_prepare(&ev), fd, addr, addrlen);
	return io_wait(&ev);
}

static inline ssize_t
io_shutdown(int fd, int how)
{
	IoEvent ev;
	io_uring_prep_shutdown(io_prepare(&ev), fd, how);
	return io_wait(&ev);
}

static inline ssize_t
io_bind(int fd, struct sockaddr* addr, socklen_t addrlen)
{
	IoEvent ev;
	io_uring_prep_bind(io_prepare(&ev), fd, addr, addrlen);
	return io_wait(&ev);
}

static inline ssize_t
io_listen(int fd, int backlog)
{
	IoEvent ev;
	io_uring_prep_listen(io_prepare(&ev), fd, backlog);
	return io_wait(&ev);
}

static inline ssize_t
io_accept(int fd, struct sockaddr* addr, socklen_t* addrlen, int flags)
{
	IoEvent ev;
	io_uring_prep_accept(io_prepare(&ev), fd, addr, addrlen, flags);
	return io_wait(&ev);
}

static inline ssize_t
io_send(int fd, const void* data, size_t len, int flags)
{
	IoEvent ev;
	io_uring_prep_send(io_prepare(&ev), fd, data, len, flags);
	return io_wait(&ev);
}

static inline ssize_t
io_recv(int fd, void* data, size_t len, int flags)
{
	IoEvent ev;
	io_uring_prep_recv(io_prepare(&ev), fd, data, len, flags);
	auto rc = io_wait(&ev);
	if (rc > 0)
		VALGRIND_MAKE_MEM_DEFINED(data, rc);
	return rc;
}

static inline ssize_t
io_poll_read(int fd)
{
	(void)fd;
	// todo:
	return 0;
}

static inline ssize_t
io_poll_write(int fd)
{
	(void)fd;
	// todo:
	return 0;
}

static inline int
io_waitid(idtype_t idtype, id_t id, siginfo_t* infop, int options)
{
	IoEvent ev;
	io_uring_prep_waitid(io_prepare(&ev), idtype, id, infop, options, 0);
	return io_wait(&ev);
}
