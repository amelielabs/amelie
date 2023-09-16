
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>

void
tcp_init(Tcp* self)
{
	self->connected     = false;
	self->eof           = false;
	self->poller        = NULL;
	self->write_iov_pos = 0;
	fd_init(&self->fd);
	tls_init(&self->tls);
	buf_init(&self->write_iov);
	buf_init(&self->readahead_buf);
	buf_pool_init(&self->write_list);
	buf_pool_init(&self->read_list);
}

void
tcp_free(Tcp* self)
{
	assert(! self->connected);
	assert(self->poller == NULL);
	buf_free(&self->readahead_buf);
	buf_free(&self->write_iov);
	buf_pool_reset(&self->write_list, mn_task->buf_cache);
	buf_pool_reset(&self->read_list, mn_task->buf_cache);
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
	self->write_iov_pos = 0;
	tls_free(&self->tls);
	tls_init(&self->tls);
	buf_reset(&self->write_iov);
	buf_pool_reset(&self->write_list, mn_task->buf_cache);
	buf_pool_reset(&self->read_list, mn_task->buf_cache);
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
	auto poller = &mn_task->poller;
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
	if (try(&e))
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

	if (catch(&e))
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
tcp_write(Tcp* self)
{
	if (buf_pool_size(&self->write_list) == 0)
		return 0;

	int iov_to_write = buf_pool_size(&self->write_list);
	if (unlikely(iov_to_write > IOV_MAX))
		iov_to_write = IOV_MAX;

	auto iov = (struct iovec*)self->write_iov.start + self->write_iov_pos;

	int rc;
	if (tls_is_set(&self->tls))
	{
		rc = tls_writev(&self->tls, iov, iov_to_write);
		if (unlikely(rc == -1))
		{
			// retry
			assert(errno == EAGAIN);
			goto done;
		}

	} else
	{
		rc = socket_writev(self->fd.fd, iov, iov_to_write);
		if (unlikely(rc == -1))
		{
			if (! (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
				error_system();

			// retry
			goto done;
		}
	}

	int written = rc;
	while (buf_pool_size(&self->write_list) > 0)
	{
		if (iov->iov_len > (size_t)written)
		{
			iov->iov_base = (char*)iov->iov_base + written;
			iov->iov_len -= written;
			break;
		}

		auto last = buf_pool_pop(&self->write_list, NULL);
		buf_cache_push(mn_task->buf_cache, last);

		written -= iov->iov_len;
		self->write_iov_pos++;
		iov++;
	}

	if (buf_pool_size(&self->write_list) == 0)
	{
		self->write_iov_pos = 0;
		buf_reset(&self->write_iov);
	}

done:
	return buf_pool_size(&self->write_list);
}

hot void
tcp_flush(Tcp* self)
{
	for (;;)
	{
		if (tcp_write(self) == 0)
			break;
		poll_write(&self->fd, -1);
	}
}

hot void
tcp_send_add(Tcp* self, Buf* buf)
{
	buf_pool_add(&self->write_list, buf);
	buf_reserve(&self->write_iov, sizeof(struct iovec));
	auto iov = (struct iovec*)self->write_iov.position;
	iov->iov_base = buf->start;
	iov->iov_len = buf_size(buf);
	buf_advance(&self->write_iov, sizeof(struct iovec));
}

void
tcp_send(Tcp* self, Buf* msg)
{
	if (msg)
		tcp_send_add(self, msg);
	tcp_flush(self);
}

void
tcp_send_list(Tcp* self, BufPool* list)
{
	Buf* buf;
	while ((buf = buf_pool_pop(list, NULL)))
		tcp_send_add(self, buf);
	tcp_flush(self);
}

hot static inline void
tcp_pipeline(Tcp* self)
{
	auto readahead = &self->readahead_buf;
	auto position = readahead->start;
	auto end = readahead->position;

	uint32_t data_left = end - position;
	while (data_left > 0)
	{
		if (data_left < sizeof(Msg))
		{
			memmove(readahead->start, position, data_left);
			readahead->position = readahead->start + data_left;
			// need more data
			return;
		}

		auto msg = (Msg*)(position);
		if (data_left < msg->size)
		{
			memmove(readahead->start, position, data_left);
			readahead->position = readahead->start + data_left;
			// need more data
			return;
		}

		// create message and add to the read list
		auto buf = buf_create(msg->size);
		memcpy(buf->start, position, msg->size);
		buf_advance(buf, msg->size);
		buf_pool_add(&self->read_list, buf);

		position += msg->size;
		data_left = end - position;
	}

	buf_reset(readahead);
}

hot static inline bool
tcp_read_tls(Tcp* self)
{
	for (;;)
	{
		buf_reserve(&self->readahead_buf, 8096);

		int rc;
		rc = tls_read(&self->tls, self->readahead_buf.position, 8096);
		if (rc == -1)
		{
			// need more data
			assert(errno == EAGAIN);
			break;
		}

		// eof
		if (rc == 0)
			return true;

		buf_advance(&self->readahead_buf, rc);

		// process more data from tls buffer
		if (! tls_read_pending(&self->tls))
			break;
	}
	return false;
}

hot static inline bool
tcp_read_socket(Tcp* self)
{
	buf_reserve(&self->readahead_buf, 8096);

	int rc;
	rc = socket_read(self->fd.fd, self->readahead_buf.position, 8096);
	if (rc == -1)
	{
		if (! (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
			error_system();

		// retry
		return false;
	}

	// eof
	if (rc == 0)
		return true;

	buf_advance(&self->readahead_buf, rc);
	return false;
}

hot static inline bool
tcp_read(Tcp* self)
{
	// read data from socket or tls
	bool eof;
	if (tls_is_set(&self->tls))
		eof = tcp_read_tls(self);
	else
		eof = tcp_read_socket(self);
	if (eof)
	{
		self->eof = true;
		return eof;
	}

	// process messages
	tcp_pipeline(self);
	return false;
}

Buf*
tcp_recv(Tcp* self)
{
	for (;;)
	{
		auto msg = buf_pool_pop(&self->read_list, &mn_self()->buf_pool);
		if (msg)
			return msg;
		poll_read(&self->fd, -1);
		bool eof = tcp_read(self);
		if (eof)
			break;
	}

	// eof
	return NULL;
}

Buf*
tcp_recv_try(Tcp* self)
{
	auto msg = buf_pool_pop(&self->read_list, &mn_self()->buf_pool);
	if (msg)
		return msg;
	bool eof;
	eof = tcp_read(self);
	if (eof)
		return NULL;
	return buf_pool_pop(&self->read_list, &mn_self()->buf_pool);
}

void
tcp_read_start(Tcp* self, Event* on_read)
{
	auto poller = &mn_task->poller;
	int rc;
	rc = poller_read(poller, &self->fd, poll_on_read_event, on_read);
	if (unlikely(rc == -1))
		error_system();
}

void
tcp_read_stop(Tcp* self)
{
	auto poller = &mn_task->poller;
	poller_read(poller, &self->fd, NULL, NULL);
}
