#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct HttpHeader HttpHeader;
typedef struct Http       Http;

typedef enum
{
	HTTP_REQUEST,
	HTTP_REPLY
} HttpType;

struct HttpHeader
{
	Str name;
	Str value;
};

struct Http
{
	HttpType  type;
	Str       method;
	Str       url;
	Str       version;
	Str       code;
	Str       msg;
	int       headers_count;
	Buf       headers;
	Buf       raw;
	Buf       content;
	Readahead readahead;
};

void http_init(Http*, HttpType, int);
void http_free(Http*);
void http_reset(Http*);
void http_log(Http*);
bool http_read(Http*, Tcp*);
void http_read_content(Http*, Tcp*, Buf*);
HttpHeader*
http_find(Http*, const char*, int);
