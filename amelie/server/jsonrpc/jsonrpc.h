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

typedef struct JsonrpcReq JsonrpcReq;
typedef struct Jsonrpc    Jsonrpc;

struct JsonrpcReq
{
	Str      method;
	uint8_t* params;
	uint8_t* id;
};

struct Jsonrpc
{
	Buf  reqs;
	int  reqs_count;
	Buf  data;
	Json parser;
};

static inline bool
jsonrpc_empty(Jsonrpc* self)
{
	return buf_empty(&self->reqs);
}

static inline JsonrpcReq*
jsonrpc_first(Jsonrpc* self)
{
	if (jsonrpc_empty(self))
		return NULL;
	return (JsonrpcReq*)self->reqs.start;
}

static inline JsonrpcReq*
jsonrpc_last(Jsonrpc* self)
{
	if (jsonrpc_empty(self))
		return NULL;
	return (JsonrpcReq*)self->reqs.start + self->reqs_count - 1;
}

void jsonrpc_init(Jsonrpc*);
void jsonrpc_free(Jsonrpc*);
void jsonrpc_reset(Jsonrpc*);
void jsonrpc_parse(Jsonrpc*, Str*);
