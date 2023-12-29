#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Tcp Tcp;

struct Tcp
{
	Fd      fd;
	bool    connected;
	bool    eof;
	Tls     tls;
	Buf     readahead_buf;
	BufPool read_list;
	Buf     write_iov;
	int     write_iov_pos;
	BufPool write_list;
	Poller* poller;
};

void tcp_init(Tcp*);
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
void tcp_flush(Tcp*);
void tcp_send_add(Tcp*, Buf*);
void tcp_send(Tcp*, Buf*);
void tcp_send_list(Tcp*, BufPool*);
Buf* tcp_recv(Tcp*);
Buf* tcp_recv_try(Tcp*);
void tcp_read_start(Tcp*, Event*);
void tcp_read_stop(Tcp*);
