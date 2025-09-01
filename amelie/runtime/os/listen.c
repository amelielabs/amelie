
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
listen_init(Listen* self)
{
	self->fd = -1;
}

static inline void
listen_socket_init(int fd, struct sockaddr* addr)
{
	int rc;
	rc = socket_set_nosigpipe(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	rc = socket_set_reuseaddr(fd, 1);
	if (unlikely(rc == -1))
		error_system();

	if (addr->sa_family != AF_UNIX)
	{
		rc = socket_set_keepalive(fd, 7200, 1);
		if (unlikely(rc == -1))
			error_system();

		rc = socket_set_nodelay(fd, 1);
		if (unlikely(rc == -1))
			error_system();
	}

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
	self->fd = socket_for(addr);
	if (unlikely(self->fd == -1))
		error_system();

	auto on_error = error_catch
	(
		// set socket options
		listen_socket_init(self->fd, addr);

		// bind
		int rc;
		rc = socket_bind(self->fd, addr);
		if (unlikely(rc == -1))
			error_system();

		// listen
		rc = socket_listen(self->fd, backlog);
		if (unlikely(rc == -1))
			error_system();
	);

	if (on_error)
	{
		socket_close(self->fd);
		self->fd = -1;
		rethrow();
	}
}

void
listen_stop(Listen* self)
{
	if (self->fd == -1)
		return;
	socket_close(self->fd);
	self->fd = -1;
}
