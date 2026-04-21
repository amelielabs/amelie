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

typedef struct Request Request;

typedef enum
{
	REQUEST_UNDEF,
	REQUEST_SQL
} RequestType;

struct Request
{
	RequestType type;
	Str         text;
	Str         rel_user;
	Str         rel;
	uint8_t*    args;
	User*       user;
	Lock*       lock;
	Endpoint    endpoint;
	Output      output;
	Jsonrpc     jsonrpc;
};

void request_init(Request*);
void request_free(Request*);
void request_reset(Request*, bool);
void request_lock(Request*, LockId);
void request_unlock(Request*);
void request_auth(Request*, Auth*);
void request_rpc(Request*, Str*);
