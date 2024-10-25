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

typedef struct Tcp Tcp;

struct Tcp
{
	Fd          fd;
	Tls         tls;
	bool        connected;
	bool        eof;
	atomic_u64* stat_sent;
	atomic_u64* stat_recv;
	Poller*     poller;
};

void tcp_init(Tcp*, atomic_u64*, atomic_u64*);
void tcp_free(Tcp*);
void tcp_close(Tcp*);
void tcp_set_tls(Tcp*, TlsContext*);
void tcp_set_fd(Tcp*, int);
void tcp_getpeername(Tcp*, char*, int);
void tcp_getsockname(Tcp*, char*, int);
void tcp_attach(Tcp*);
void tcp_detach(Tcp*);
bool tcp_connected(Tcp*);
bool tcp_eof(Tcp*);
void tcp_connect_fd(Tcp*);
void tcp_connect(Tcp*, struct sockaddr*);
int  tcp_read(Tcp*, Buf*, int);
void tcp_write(Tcp*, struct iovec*, int);
void tcp_write_buf(Tcp*, Buf*);
void tcp_write_pair(Tcp*, Buf*, Buf*);
void tcp_write_pair_str(Tcp*, Buf*, Str*);
