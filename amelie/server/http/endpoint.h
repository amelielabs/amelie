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
	Opt  proto;
	// auth
	Opt  user;
	Opt  token;
	Opt  token_type;
	// host
	Opt  host;
	Opt  port;
	// repository
	Opt  path;
	// tls
	Opt  tls_capath;
	Opt  tls_ca;
	Opt  tls_cert;
	Opt  tls_key;
	Opt  tls_server;
	// endpoint
	Opt  uri;
	Opt  content_type;
	Opt  accept;
	Opt  endpoint;
	// context
	Opt  timezone;
	Opt  time;
	Opt  seed;
	Opt  id;
	// misc
	Opt  jsonrpc;
	Opt  trusted;
	Opt  name;
	Opt  debug;
	Opts opts;
};

void endpoint_init(Endpoint*);
void endpoint_free(Endpoint*);
void endpoint_reset(Endpoint*);
void endpoint_auth(Endpoint*);
