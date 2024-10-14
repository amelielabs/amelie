#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct TlsContext TlsContext;

struct TlsContext
{
	bool    client;
	void*   ctx;
	Remote* remote;
};

void tls_context_init(TlsContext*);
void tls_context_free(TlsContext*);
bool tls_context_created(TlsContext*);
void tls_context_create(TlsContext*, bool, Remote*);
