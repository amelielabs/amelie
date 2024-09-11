#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ServerListen ServerListen;

struct ServerListen
{
	Listen             listen;
	struct addrinfo*   addr;
	Str                addr_name;
	struct sockaddr_un sa_un;
	struct sockaddr*   sa;
	ServerConfig*      config;
	List               link;
};

static inline ServerListen*
server_listen_allocate(ServerConfig* config)
{
	ServerListen* self;
	self = (ServerListen*)am_malloc(sizeof(*self));
	self->addr   = NULL;
	self->sa     = NULL;
	self->config = config;
	memset(&self->sa_un, 0, sizeof(self->sa_un));
	str_init(&self->addr_name);
	listen_init(&self->listen);
	list_init(&self->link);
	return self;
}

static inline void
server_listen_free(ServerListen* self)
{
	str_free(&self->addr_name);
	am_free(self);
}
