#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct TlsContext TlsContext;

enum
{
	TLS_SERVER,
	TLS_PROTOCOLS,
	TLS_PATH_CA,
	TLS_FILE_CA,
	TLS_FILE_CERT,
	TLS_FILE_KEY,
	TLS_MAX
};

struct TlsContext
{
	bool  client;
	Str   options[TLS_MAX];
	void* ctx;
};

void  tls_context_init(TlsContext*, bool);
void  tls_context_free(TlsContext*);
void  tls_context_set(TlsContext*, int, Str*);
void  tls_context_set_path(TlsContext*, int, const char*, Str*);
char* tls_context_get(TlsContext*, int);
void  tls_context_create(TlsContext*);
