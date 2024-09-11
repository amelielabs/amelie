#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Client Client;

struct Client
{
	Http       request;
	Http       reply;
	Readahead  readahead;
	Tcp        tcp;
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
void client_set_task_name(Client*);
void client_set_remote(Client*, Remote*);
void client_attach(Client*);
void client_detach(Client*);
void client_accept(Client*);
void client_connect(Client*);
void client_close(Client*);
void client_execute(Client*, Str*);
