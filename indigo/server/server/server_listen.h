#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct ServerListen ServerListen;

struct ServerListen
{
	Listen           listen;
	uint64_t         worker;
	struct addrinfo* addr;
	bool             tls;
	TlsContext*      tls_context;
	ServerConfig*    config;
	void*            arg;
	List             link;
};

static inline ServerListen*
server_listen_allocate(ServerConfig* config)
{
	ServerListen* self;
	self = (ServerListen*)mn_malloc(sizeof(*self));
	self->worker      = UINT64_MAX;
	self->addr        = NULL;
	self->tls         = false;
	self->tls_context = NULL;
	self->config      = config;
	self->arg         = NULL;
	listen_init(&self->listen);
	list_init(&self->link);
	return self;
}

static inline void
server_listen_free(ServerListen* self)
{
	mn_free(self);
}
