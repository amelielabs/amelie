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

typedef struct Client Client;

struct Client
{
	Http       request;
	Http       reply;
	Readahead  readahead;
	Tcp        tcp;
	bool       auth;
	TlsContext tls_context;
	UriHost*   host;
	Uri        uri;
	Remote*    remote;
	uint64_t   coroutine_id;
	void*      arg;
	List       link;
};

Client*
client_create(void);
void client_free(Client*);
void client_set_coroutine_name(Client*);
void client_set_remote(Client*, Remote*);
void client_set_auth(Client*, bool);
void client_attach(Client*);
void client_detach(Client*);
void client_accept(Client*);
void client_connect(Client*);
void client_close(Client*);
void client_execute(Client*, Str*);
void client_import(Client*, Str*, Str*, Str*);
