#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
void tls_context_create(TlsContext*, bool, Remote*);
