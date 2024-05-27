
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>

void
tcp_init(Tcp* self)
{
	self->connected = false;
	self->eof       = false;
	self->poller    = NULL;
	fd_init(&self->fd);
	tls_init(&self->tls);
}

void
tcp_free(Tcp* self)
{
	assert(! self->connected);
	assert(self->poller == NULL);
}

void
tcp_close(Tcp* self)
{
	if (unlikely(self->fd.fd != -1))
	{
		if (self->poller)
			poller_del(self->poller, &self->fd);
		socket_close(self->fd.fd);
	}
	self->connected     = false;
	self->eof           = false;
	self->poller        = NULL;
	self->fd.fd         = -1;
	tls_free(&self->tls);
	tls_init(&self->tls);
}

void
tcp_set_tls(Tcp* self, TlsContext *context)
{
	tls_set(&self->tls, context);
}

static inline void
tcp_socket_init(int fd)
{
	int rc;
	rc = socket_set_nonblock(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	rc = socket_set_keepalive(fd, 7200, 1);
	if (unlikely(rc == -1))
		error_system();

	rc = socket_set_nodelay(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	rc = socket_set_nosigpipe(fd, 1);
	if (unlikely(rc == -1))
		error_system();
}

void
tcp_set_fd(Tcp* self, int fd)
{
	assert(self->poller == NULL);
	tcp_socket_init(fd);
	self->fd.fd = fd;
}

void
tcp_getpeername(Tcp* self, char* buf, int size)
{
	struct sockaddr_storage sa;
	socklen_t salen = sizeof(sa);
	int rc;
	rc = socket_getpeername(self->fd.fd, (struct sockaddr *)&sa, &salen);
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
	rc = socket_getsockname(self->fd.fd, (struct sockaddr *)&sa, &salen);
	if (unlikely(rc == -1))
		error_system();
	socket_getaddrname((struct sockaddr *)&sa, buf, size, true, true);
}

void
tcp_attach(Tcp* self)
{
	assert(self->poller == NULL);
	assert(self->fd.fd != -1);
	auto poller = &so_task->poller;
	int rc;
	rc = poller_add(poller, &self->fd);
	if (unlikely(rc == -1))
		error_system();
	self->poller = poller;
}

void
tcp_detach(Tcp* self)
{
	if (self->poller == NULL)
		return;
	poller_del(self->poller, &self->fd);
	self->poller = NULL;
}

void
tcp_connect_fd(Tcp* self)
{
	if (tls_is_set(&self->tls))
	{
		tls_create(&self->tls, &self->fd);
		tls_handshake(&self->tls);
	}
	self->connected = true;
}

static inline void
tcp_connect_begin(Tcp* self, struct sockaddr* addr)
{
	assert(self->poller == NULL);
	assert(self->fd.fd == -1);

	self->fd.fd = socket_for(addr);
	if (unlikely(self->fd.fd == -1))
		error("socket(): %s", strerror(errno));
	self->connected = false;

	Exception e;
	if (enter(&e))
	{
		// set socket options
		tcp_socket_init(self->fd.fd);

		// start async connection
		int rc;
		rc = socket_connect(self->fd.fd, addr);
		if (rc == 0) {
			self->connected = true;
		} else
		{
			assert(rc == -1);
			if (errno != EINPROGRESS)
				error("connect(): %s", strerror(errno));
			tcp_attach(self);
		}
	}

	if (leave(&e))
	{
		tcp_close(self);
		rethrow();
	}
}

static inline void
tcp_connect_complete(Tcp* self)
{
	int status = socket_error(self->fd.fd);
	if (unlikely(status != 0))
	{
		tcp_close(self);
		error("connect(): %s", strerror(status));
	}
	if (tls_is_set(&self->tls))
	{
		tls_create(&self->tls, &self->fd);
		tls_handshake(&self->tls);
	}
	self->connected = true;
}

void
tcp_connect(Tcp* self, struct sockaddr *addr)
{
	tcp_connect_begin(self, addr);
	if (self->connected)
		return;
	poll_write(&self->fd, -1);
	tcp_connect_complete(self);
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

static inline int
tcp_read_tls(Tcp* self, Buf* buf, int size)
{
	int rc;
	rc = tls_read(&self->tls, buf, size);
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

static inline int
tcp_read_socket(Tcp* self, Buf* buf, int size)
{
	int rc;
	rc = socket_read(self->fd.fd, buf->position, size);
	if (rc == -1)
	{
		if (! (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
			error_system();
		// retry
		return -1;
	}

	// might be zero in case of eof
	buf_advance(buf, rc);
	return rc;
}

int
tcp_read(Tcp* self, Buf* buf, int size)
{
	bool poll = true;
	if (tls_is_set(&self->tls) && tls_read_pending(&self->tls))
		poll = false;
	if (poll)
		poll_read(&self->fd, -1);

	buf_reserve(buf, size);

	// read data from socket or tls
	int rc;
	if (tls_is_set(&self->tls))
		rc = tcp_read_tls(self, buf, size);
	else
		rc = tcp_read_socket(self, buf, size);

	// eof
	if (rc == 0)
		self->eof = true;

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
			if (unlikely(rc == -1)) {
				assert(errno == EAGAIN);
			}
		} else
		{
			rc = socket_writev(self->fd.fd, iov, iov_count);
			if (unlikely(rc == -1))
			{
				if (! (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
					error_system();
			}
		}

		// retry
		if (rc == -1)
		{
			poll_write(&self->fd, -1);
			continue;
		}

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

void
tcp_write_buf(Tcp* self, Buf* buf)
{
	struct iovec iov =
	{
		.iov_base = buf->start,
		.iov_len  = buf_size(buf)
	};
	tcp_write(self, &iov, 1);
}
