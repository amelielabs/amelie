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

typedef struct UriHost UriHost;
typedef struct UriArg  UriArg;
typedef struct Uri     Uri;

typedef enum
{
	URI_HTTP,
	URI_HTTPS,
} UriProto;

struct UriHost
{
	Str  host;
	int  port;
	List link;
};

struct UriArg
{
	Str  name;
	Str  value;
	List link;
};

struct Uri
{
	UriProto proto;
	Str      user;
	Str      password;
	Str      path;
	Str      uri;
	int      hosts_count;
	List     hosts;
	int      args_count;
	List     args;
	char*    pos;
};

void uri_init(Uri*);
void uri_free(Uri*);
void uri_reset(Uri*);
void uri_set(Uri*, Str*, bool);
UriArg*
uri_find(Uri*, Str*);
