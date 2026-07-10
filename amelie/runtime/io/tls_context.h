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

typedef struct TlsContext TlsContext;

struct TlsContext
{
	void* ctx;
	bool  client;
	Str*  file_cert;
	Str*  file_key;
	Str*  file_ca;
	Str*  path_ca;
	Str*  server;
};

void tls_context_init(TlsContext*);
void tls_context_free(TlsContext*);
void tls_context_set(TlsContext*, bool, Str*, Str*, Str*, Str*, Str*);
bool tls_context_created(TlsContext*);
void tls_context_create(TlsContext*);
