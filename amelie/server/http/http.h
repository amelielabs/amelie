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

typedef struct HttpHeader HttpHeader;
typedef struct Http       Http;

typedef enum
{
	HTTP_METHOD,
	HTTP_URL,
	HTTP_VERSION,
	HTTP_CODE,
	HTTP_MSG,
	HTTP_MAX
} HttpOption;

struct HttpHeader
{
	Str name;
	Str value;
};

struct Http
{
	Str options[HTTP_MAX];
	int headers_count;
	Buf headers;
	Buf raw;
	Buf content;
};

void http_init(Http*);
void http_free(Http*);
void http_reset(Http*);
void http_log(Http*);
bool http_read(Http*, Readahead*, bool);
void http_read_content(Http*, Readahead*, Buf*);
bool http_read_content_limit(Http*, Readahead*, Buf*, uint64_t);
void http_write_request(Http*, char*, ...);
void http_write_reply(Http*, int, char*);
void http_write(Http*, char*, char*, ...);
void http_write_end(Http*);
HttpHeader*
http_find(Http*, char*, int);
