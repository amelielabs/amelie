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
};

struct Endpoint
{
	// protocol
	Opt  proto;
	// auth
	Opt  user;
	Opt  secret;
	Opt  token;
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
	Opt  service;
	Opt  db;
	Opt  table;
	Opt  function;
	Opt  columns;
	// args
	Opt  timezone;
	Opt  ret;
	// misc
	Opt  name;
	Opt  debug;
	Opts opts;
};

void endpoint_init(Endpoint*);
void endpoint_free(Endpoint*);
void endpoint_reset(Endpoint*);
void endpoint_copy(Endpoint*, Endpoint*);
void endpoint_read(Endpoint*, uint8_t**);
void endpoint_write(Endpoint*, Buf*);
