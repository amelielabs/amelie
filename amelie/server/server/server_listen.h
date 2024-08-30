#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ServerListen ServerListen;

struct ServerListen
{
	Listen           listen;
	uint64_t         worker;
	struct addrinfo* addr;
	ServerConfig*    config;
	void*            arg;
	List             link;
};

static inline ServerListen*
server_listen_allocate(ServerConfig* config)
{
	ServerListen* self;
	self = (ServerListen*)am_malloc(sizeof(*self));
	self->worker = UINT64_MAX;
	self->addr   = NULL;
	self->config = config;
	self->arg    = NULL;
	listen_init(&self->listen);
	list_init(&self->link);
	return self;
}

static inline void
server_listen_free(ServerListen* self)
{
	am_free(self);
}
