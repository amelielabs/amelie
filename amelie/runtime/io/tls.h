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

typedef struct Tls Tls;

struct Tls
{
	Fd*         fd;
	Buf         write_buf;
	void*       ssl;
	TlsContext* context;
};

void tls_init(Tls*);
void tls_free(Tls*);
void tls_error(Tls*, int, const char*, ...);
bool tls_is_set(Tls*);
void tls_set(Tls*, TlsContext*);
void tls_create(Tls*, Fd*);
void tls_handshake(Tls*);
bool tls_read_pending(Tls*);
int  tls_read(Tls*, void*, int);
int  tls_write(Tls*, void*, int);
int  tls_writev(Tls*, struct iovec*, int);
int  tls_explain(Tls*, char*, int);
