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

typedef struct Server Server;

typedef void (*ServerEvent)(Server*, Client*);

struct Server
{
	Listen           listen;
	int64_t          worker;
	struct addrinfo* addr;
	Str              addr_name;
	ServerConfig*    config;
	ServerEvent      on_connect;
	void*            on_connect_arg;
	List             link;
};

Server*
server_allocate(ServerConfig*, ServerEvent, void*);
void server_free(Server*);
void server_listen(Server*);
void server_start(Server*);
void server_stop(Server*);
