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
	int         fd;
	Tls         tls;
	bool        connected;
	bool        eof;
	atomic_u64* stat_sent;
	atomic_u64* stat_recv;
};

void tcp_init(Tcp*, atomic_u64*, atomic_u64*);
void tcp_free(Tcp*);
void tcp_close(Tcp*);
void tcp_set_tls(Tcp*, TlsContext*);
void tcp_set_fd(Tcp*, int);
void tcp_getpeername(Tcp*, char*, int);
void tcp_getsockname(Tcp*, char*, int);
bool tcp_connected(Tcp*);
bool tcp_eof(Tcp*);
void tcp_connect_fd(Tcp*);
void tcp_connect(Tcp*, struct sockaddr*);
int  tcp_read(Tcp*, Buf*, int);
void tcp_write(Tcp*, struct iovec*, int);

static inline void
tcp_write_buf(Tcp* self, Buf* buf)
{
	struct iovec iov =
	{
		.iov_base = buf->start,
		.iov_len  = buf_size(buf)
	};
	tcp_write(self, &iov, 1);
}

static inline void
tcp_write_pair(Tcp* self, Buf* a, Buf* b)
{
	struct iovec iov[2] =
	{
		{
			.iov_base = a->start,
			.iov_len  = buf_size(a)
		},
		{
			.iov_base = b->start,
			.iov_len  = buf_size(b)
		},
	};
	tcp_write(self, iov, 2);
}

static inline void
tcp_write_pair_str(Tcp* self, Buf* a, Str* b)
{
	struct iovec iov[2] =
	{
		{
			.iov_base = a->start,
			.iov_len  = buf_size(a)
		},
		{
			.iov_base = b->pos,
			.iov_len  = str_size(b)
		},
	};
	tcp_write(self, iov, 2);
}
