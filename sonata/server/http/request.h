#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct RequestHeader RequestHeader;
typedef struct Request       Request;

struct RequestHeader
{
	Str name;
	Str value;
};

struct Request
{
	Str       method;
	Str       url;
	Str       version;
	Str       content;
	int       headers_count;
	Buf       headers;
	Buf       raw;
	Readahead readahead;
};

void request_init(Request*);
void request_free(Request*);
void request_reset(Request*);
void request_log(Request*);
void request_read(Request*, Tcp*);
