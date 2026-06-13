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

typedef struct Query Query;
typedef struct Api   Api;

typedef enum
{
	API_UNDEF,
	API_SQL,
	API_WRITE,
	API_FOLLOW,
	API_UNFOLLOW
} ApiType;

struct Api
{
	ApiType  type;
	Str      text;
	Str      rel_user;
	Str      rel;
	uint8_t* args;
	int      args_size;
	Request* request;
	Jsonrpc  jsonrpc;
};

void api_init(Api*, Request*);
void api_free(Api*);
void api_reset(Api*);
bool api_parse(Api*, Str*, Query*, bool);
