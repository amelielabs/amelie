
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>

void
listen_init(Listen* self)
{
	self->poller = NULL;
	fd_init(&self->fd);
}

static inline void
listen_socket_init(int fd, struct sockaddr* addr)
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

	rc = socket_set_reuseaddr(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	if (addr->sa_family == AF_INET6)
	{
		rc = socket_set_ipv6only(fd, 1);
		if (unlikely(rc == -1))
			error_system();
	}
}

void
listen_start(Listen* self, int backlog, struct sockaddr* addr)
{
	auto poller = &mn_task->poller;

	self->fd.fd = socket_for(addr);
	if (unlikely(self->fd.fd == -1))
		error_system();

	self->poller = poller;

	Exception e;
	if (try(&e))
	{
		// set socket options
		listen_socket_init(self->fd.fd, addr);

		// bind
		int rc;
		rc = socket_bind(self->fd.fd, addr);
		if (unlikely(rc == -1))
			error_system();

		// listen
		rc = socket_listen(self->fd.fd, backlog);
		if (unlikely(rc == -1))
			error_system();

		// add socket to the task poller
		rc = poller_add(poller, &self->fd);
		if (unlikely(rc == -1))
			error_system();
	}

	if (catch(&e))
	{
		socket_close(self->fd.fd);
		self->fd.fd = -1;
		rethrow();
	}
}

void
listen_stop(Listen* self)
{
	if (self->fd.fd == -1)
		return;
	if (self->poller)
	{
		poller_del(self->poller, &self->fd);
		self->poller = NULL;
	}
	socket_close(self->fd.fd);
	self->fd.fd = -1;
}

int
listen_accept(Listen* self)
{
	for (;;)
	{
		poll_read(&self->fd, -1);

		int client_fd;
		client_fd = socket_accept(self->fd.fd, NULL, NULL);
		if (unlikely(client_fd == -1))
			continue;

		return client_fd;
	}

	// unreach
	return 0;
}
