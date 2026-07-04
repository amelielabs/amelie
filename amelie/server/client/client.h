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
	Msg        msg;
	Http       request;
	Http       reply;
	Readahead  readahead;
	Endpoint*  endpoint;
	Tcp        tcp;
	TlsContext tls_context;
	bool       trusted;
	bool       accepted;
	uint64_t   coroutine_id;
	void*      arg;
	Histogram* histogram;
	List       link;
};

Client*
client_allocate(void);
void client_free(Client*);
void client_set_coroutine_name(Client*);
void client_set_endpoint(Client*, Endpoint*);
void client_set_trusted(Client*, bool);
void client_set_histogram(Client*, Histogram*);
void client_attach(Client*);
void client_detach(Client*);
void client_accept(Client*);
void client_connect(Client*);
void client_close(Client*);
