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

int  socket_for(struct sockaddr*);
int  socket_set_nonblock(int, int);
int  socket_set_nodelay(int, int);
int  socket_set_keepalive(int, int, int);
int  socket_set_nosigpipe(int, int);
int  socket_set_reuseaddr(int, int);
int  socket_set_ipv6only(int, int);
int  socket_error(int);
int  socket_close(int);
int  socket_connect(int, struct sockaddr*);
int  socket_bind(int, struct sockaddr*);
int  socket_listen(int, int);
int  socket_accept(int, struct sockaddr*, socklen_t*);
int  socket_write(int, void*, int);
int  socket_writev(int, struct iovec*, int);
int  socket_read(int, void*, int);
int  socket_getsockname(int, struct sockaddr*, socklen_t*);
int  socket_getpeername(int, struct sockaddr*, socklen_t*);
int  socket_getaddrinfo(const char*, const char*, struct addrinfo*, struct addrinfo**);
void socket_getaddrname(struct sockaddr*, char*, int, bool, bool);

