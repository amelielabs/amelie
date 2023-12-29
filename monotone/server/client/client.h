#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Client Client;

struct Client
{
	Tcp         tcp;
	Access      access;
	AccessMode  mode;
	Auth        auth;
	UriHost*    host;
	Uri         uri;
	uint64_t    coroutine_id;
	TlsContext* tls_context;
	void*       arg;
	List        link;
};

Client*
client_create(Access);
void client_free(Client*);
void client_set_access(Client*, Access);
void client_set_access_mode(Client*, AccessMode);
void client_set_uri(Client*, bool, Str*);
void client_set_coroutine_name(Client*);
void client_attach(Client*);
void client_detach(Client*);
void client_accept(Client*, UserCache*);
void client_connect(Client*);
void client_close(Client*);
