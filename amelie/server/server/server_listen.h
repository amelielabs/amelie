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

typedef struct ServerListen ServerListen;

struct ServerListen
{
	Listen           listen;
	uint64_t         worker;
	struct addrinfo* addr;
	Str              addr_name;
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
	str_init(&self->addr_name);
	listen_init(&self->listen);
	list_init(&self->link);
	return self;
}

static inline void
server_listen_free(ServerListen* self)
{
	listen_stop(&self->listen);
	str_free(&self->addr_name);
	am_free(self);
}
