
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
socket_for(struct sockaddr* sa)
{
	return socket(sa->sa_family, SOCK_STREAM, 0);
}

int
socket_set_nonblock(int fd, int enable)
{
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1)
		return -1;
	if (enable)
		flags |= O_NONBLOCK;
	else
		flags &= ~O_NONBLOCK;
	int rc;
	rc = fcntl(fd, F_SETFL, flags);
	return rc;
}

int
socket_set_nodelay(int fd, int enable)
{
	int rc;
	rc = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enable,
	                sizeof(enable));
	return rc;
}

int
socket_set_keepalive(int fd, int enable, int delay)
{
	int rc;
	rc = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enable,
	                sizeof(enable));
	if (rc == -1)
		return -1;
#ifdef TCP_KEEPIDLE
	if (enable) {
		rc = setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &delay,
		                sizeof(delay));
		if (rc == -1)
			return -1;
	}
#endif
	return 0;
}

int
socket_set_nosigpipe(int fd, int enable)
{
#if defined(SO_NOSIGPIPE)
	int enable = 1;
	rc = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enable,
	                sizeof(enable));
	if (rc == -1)
		return -1;
#endif
	(void)fd;
	(void)enable;
	return 0;
}

int
socket_set_reuseaddr(int fd, int enable)
{
	int rc;
	rc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable,
	                sizeof(enable));
	return rc;
}

int
socket_set_ipv6only(int fd, int enable)
{
	int rc;
	rc = setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &enable,
	                sizeof(enable));
	return rc;
}

int
socket_error(int fd)
{
	int error;
	socklen_t errorsize = sizeof(error);
	int rc;
	rc = getsockopt(fd, SOL_SOCKET, SO_ERROR, &error,
	                &errorsize);
	if (rc == -1)
		return -1;
	return error;
}

int
socket_close(int fd)
{
	int rc = close(fd);
	return rc;
}

int
socket_connect(int fd, struct sockaddr* sa)
{
	int addrlen;
	if (sa->sa_family == AF_INET) {
		addrlen = sizeof(struct sockaddr_in);
	} else
	if (sa->sa_family == AF_INET6) {
		addrlen = sizeof(struct sockaddr_in6);
	} else
	if (sa->sa_family == AF_UNIX) {
		addrlen = sizeof(struct sockaddr_un);
	} else {
		errno = EINVAL;
		return -1;
	}
	int rc;
	rc = connect(fd, sa, addrlen);
	return rc;
}

int
socket_bind(int fd, struct sockaddr* sa)
{
	int addrlen;
	if (sa->sa_family == AF_INET) {
		addrlen = sizeof(struct sockaddr_in);
	} else
	if (sa->sa_family == AF_INET6) {
		addrlen = sizeof(struct sockaddr_in6);
	} else
	if (sa->sa_family == AF_UNIX) {
		addrlen = sizeof(struct sockaddr_un);
	} else {
		errno = EINVAL;
		return -1;
	}
	int rc;
	rc = bind(fd, sa, addrlen);
	return rc;
}

int
socket_listen(int fd, int backlog)
{
	int rc;
	rc = listen(fd, backlog);
	return rc;
}

int
socket_accept(int fd, struct sockaddr* sa, socklen_t* slen)
{
	int rc;
	rc = accept(fd, sa, slen);
	return rc;
}

int
socket_write(int fd, void* buf, int size)
{
	int rc;
	rc = write(fd, buf, size);
	return rc;
}

int
socket_writev(int fd, struct iovec* iov, int iovc)
{
	int rc;
	rc = writev(fd, iov, iovc);
	return rc;
}

int
socket_read(int fd, void* buf, int size)
{
	int rc;
	rc = read(fd, buf, size);
	return rc;
}

int
socket_getsockname(int fd, struct sockaddr* sa, socklen_t* salen)
{
	int rc;
	rc = getsockname(fd, sa, salen);
	return rc;
}

int
socket_getpeername(int fd, struct sockaddr* sa, socklen_t* salen)
{
	int rc;
	rc = getpeername(fd, sa, salen);
	return rc;
}

int
socket_getaddrinfo(const char*       node,
                   const char*       service,
                   struct addrinfo*  hints,
                   struct addrinfo** res)
{
	int rc;
	rc = getaddrinfo(node, service, hints, res);
	return rc;
}

void
socket_getaddrname(struct sockaddr* sa, char* buf, int size,
                   bool with_addr,
                   bool with_port)
{
	char addr[128];
	if (sa->sa_family == AF_INET)
	{
		struct sockaddr_in* sin = (struct sockaddr_in *)sa;
		inet_ntop(sa->sa_family, &sin->sin_addr, addr, sizeof(addr));
		if (with_addr && with_port)
			snprintf(buf, size, "%s:%d", addr, ntohs(sin->sin_port));
		else if (with_addr)
			snprintf(buf, size, "%s", addr);
		else if (with_port)
			snprintf(buf, size, "%d", ntohs(sin->sin_port));
		return;
	}
	if (sa->sa_family == AF_INET6)
	{
		struct sockaddr_in6* sin = (struct sockaddr_in6 *)sa;
		inet_ntop(sa->sa_family, &sin->sin6_addr, addr, sizeof(addr));
		if (with_addr && with_port)
			snprintf(buf, size, "[%s]:%d", addr, ntohs(sin->sin6_port));
		else if (with_addr)
			snprintf(buf, size, "%s", addr);
		else if (with_port)
			snprintf(buf, size, "%d", ntohs(sin->sin6_port));
		return;
	}
	if (sa->sa_family == AF_UNIX)
	{
		snprintf(buf, size, "<unix socket>");
		return;
	}
	snprintf(buf, size, "%s", "");
}
