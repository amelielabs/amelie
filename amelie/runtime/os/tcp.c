
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

#include <amelie_base.h>
#include <amelie_os.h>

void
tcp_init(Tcp* self, atomic_u64* stat_sent, atomic_u64* stat_recv)
{
	self->fd        = -1;
	self->connected = false;
	self->eof       = false;
	self->stat_sent = stat_sent;
	self->stat_recv = stat_recv;
	tls_init(&self->tls);
}

void
tcp_free(Tcp* self)
{
	unused(self);
	assert(! self->connected);
}

void
tcp_close(Tcp* self)
{
	if (unlikely(self->fd != -1))
	{
		io_close(self->fd);
		self->fd = -1;
	}
	self->connected = false;
	self->eof       = false;
	tls_free(&self->tls);
	tls_init(&self->tls);
}

void
tcp_set_tls(Tcp* self, TlsContext *context)
{
	tls_set(&self->tls, context);
}

static inline void
tcp_socket_init(int fd, int family)
{
	int rc;
	rc = socket_set_nonblock(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	rc = socket_set_nosigpipe(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	if (family != AF_UNIX)
	{
		rc = socket_set_keepalive(fd, 7200, 1);
		if (unlikely(rc == -1))
			error_system();

		rc = socket_set_nodelay(fd, 1);
		if (unlikely(rc == -1))
			error_system();
	}
}

void
tcp_set_fd(Tcp* self, int fd)
{
	struct sockaddr_storage sa;
	socklen_t salen = sizeof(sa);
	int rc;
	rc = socket_getsockname(fd, (struct sockaddr*)&sa, &salen);
	if (unlikely(rc == -1))
		error_system();

	int family = ((struct sockaddr*)&sa)->sa_family;
	tcp_socket_init(fd, family);
	self->fd = fd;
}

void
tcp_getpeername(Tcp* self, char* buf, int size)
{
	struct sockaddr_storage sa;
	socklen_t salen = sizeof(sa);
	int rc;
	rc = socket_getpeername(self->fd, (struct sockaddr*)&sa, &salen);
	if (unlikely(rc == -1))
		error_system();
	socket_getaddrname((struct sockaddr *)&sa, buf, size, true, true);
}

void
tcp_getsockname(Tcp* self, char* buf, int size)
{
	struct sockaddr_storage sa;
	socklen_t salen = sizeof(sa);
	int rc;
	rc = socket_getsockname(self->fd, (struct sockaddr*)&sa, &salen);
	if (unlikely(rc == -1))
		error_system();
	socket_getaddrname((struct sockaddr*)&sa, buf, size, true, true);
}

void
tcp_connect_fd(Tcp* self)
{
	if (tls_is_set(&self->tls))
	{
		tls_create(&self->tls, self->fd);
		tls_handshake(&self->tls);
	}
	self->connected = true;
}

void
tcp_connect(Tcp* self, struct sockaddr* addr)
{
	// prepare socket
	assert(self->fd == -1);
	self->fd = io_socket(addr->sa_family, SOCK_STREAM, 0, 0);
	if (unlikely(self->fd == -1))
		error_system();
	tcp_socket_init(self->fd, addr->sa_family);

	// connect
	auto rc = io_connect(self->fd, addr);
	if (rc == -1)
	{
		tcp_close(self);
		error("connect(): %s", strerror(errno));
	}
	tcp_connect_fd(self);
}

bool
tcp_connected(Tcp* self)
{
	return self->connected;
}

bool
tcp_eof(Tcp* self)
{
	return self->eof;
}

int
tcp_read(Tcp* self, Buf* buf, int size)
{
	buf_reserve(buf, size);

	// read data from socket or tls
	ssize_t rc;
	if (tls_is_set(&self->tls))
	{
		bool poll = true;
		if (tls_is_set(&self->tls) && tls_read_pending(&self->tls))
			poll = false;
		if (poll)
			io_poll_read(self->fd);

		rc = tls_read(&self->tls, buf->position, size);
		if (rc == -1)
		{
			// need more data
			assert(errno == EAGAIN);
			return -1;
		}

		// might be zero in case of eof
		buf_advance(buf, rc);
		return rc;
	}

	// treat socket errors as eof
	rc = io_recv(self->fd, buf->position, size, 0);
	if (rc == -1)
		rc = 0;

	// might be zero in case of eof
	buf_advance(buf, rc);

	// eof
	if (rc == 0)
		self->eof = true;

	atomic_u64_add(self->stat_recv, rc);
	return rc;
}

void
tcp_write(Tcp* self, struct iovec* iov, int iov_count)
{
	while (iov_count > 0)
	{
		int rc;
		if (tls_is_set(&self->tls))
		{
			rc = tls_writev(&self->tls, iov, iov_count);

			// retry
			if (unlikely(rc == -1))
			{
				assert(errno == EAGAIN);
				io_poll_write(self->fd);
				continue;
			}
		} else
		{
			rc = io_pwritev(self->fd, iov, iov_count, 0);
			if (rc == -1)
				error_system();
		}

		// update stats
		atomic_u64_add(self->stat_sent, rc);

		while (iov_count > 0)
		{
			if (iov->iov_len > (size_t)rc)
			{
				iov->iov_base = (char*)iov->iov_base + rc;
				iov->iov_len -= rc;
				break;
			}
			rc -= iov->iov_len;
			iov++;
			iov_count--;
		}
	}
}
