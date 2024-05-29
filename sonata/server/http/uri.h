#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
void uri_set(Uri*, Str*);
UriArg*
uri_find(Uri*, const char*, int);
