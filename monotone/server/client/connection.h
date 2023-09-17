#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Connection Connection;

struct Connection
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

Connection*
connection_create(Access);
void connection_free(Connection*);
void connection_set_access(Connection*, Access);
void connection_set_access_mode(Connection*, AccessMode);
void connection_set_uri(Connection*, bool, Str*);
void connection_set_coroutine_name(Connection*);
void connection_attach(Connection*);
void connection_detach(Connection*);
void connection_accept(Connection*, UserMgr*);
void connection_connect(Connection*);
void connection_close(Connection*);
