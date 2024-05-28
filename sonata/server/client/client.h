#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Client Client;

struct Client
{
	Tcp         tcp;
	TlsContext* tls_context;
	UriHost*    host;
	Uri         uri;
	uint64_t    coroutine_id;
	void*       arg;
	List        link;
};

Client*
client_create(void);
void client_free(Client*);
void client_set_coroutine_name(Client*);
void client_set_uri(Client*, Str*);
void client_attach(Client*);
void client_detach(Client*);
void client_accept(Client*);
void client_connect(Client*);
void client_close(Client*);
