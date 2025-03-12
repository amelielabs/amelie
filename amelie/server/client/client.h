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
	Readahead  readahead;
	Tcp        tcp;
	bool       auth;
	TlsContext tls_context;
	UriHost*   host;
	Uri        uri;
	Remote*    remote;
	void*      arg;
	List       link;
};

Client*
client_create(void);
void client_free(Client*);
void client_set_remote(Client*, Remote*);
void client_set_auth(Client*, bool);
void client_attach(Client*);
void client_detach(Client*);
void client_accept(Client*);
void client_connect(Client*);
void client_close(Client*);

hot static inline Client*
client_of(Fd* fd)
{
	return container_of(container_of(fd, Tcp, fd), Client, tcp);
}
