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

typedef struct Endpoint Endpoint;

enum
{
	PROTO_HTTP,
	PROTO_HTTPS,
	PROTO_AMELIE,
	PROTO_AMELIES
};

enum
{
	TOKEN_NONE,
	TOKEN_BASIC,
	TOKEN_JWT
};

struct Endpoint
{
	// protocol
	uint32_t proto;

	// auth
	Str      user;
	Str      token;
	uint32_t token_type;

	// host
	Str      host;
	uint32_t port;

	// repository
	Str      path;

	// tls
	Str      tls_capath;
	Str      tls_ca;
	Str      tls_cert;
	Str      tls_key;
	Str      tls_server;

	// endpoint
	Str      uri;
	Str      content_type;
	Str      accept;

	// relation
	Str      db;
	Uuid     db_id;
	Str      rel;

	// context
	Str      timezone;
	uint64_t time;
	uint64_t seed;
	Str      id;

	// misc
	Str      jsonrpc;
	bool     trusted;
	bool     debug;

	// options
	Opts     opts;
};

void endpoint_init(Endpoint*);
void endpoint_free(Endpoint*);
void endpoint_reset(Endpoint*);
void endpoint_auth(Endpoint*);
