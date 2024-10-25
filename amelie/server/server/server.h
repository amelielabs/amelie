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
	TlsContext  tls;
	Remote      tls_remote;
	List        listen;
	int         listen_count;
	List        config;
	int         config_count;
	ServerEvent on_connect;
	void*       on_connect_arg;
};

void server_init(Server*);
void server_free(Server*);
void server_open(Server*);
void server_start(Server*, ServerEvent, void*);
void server_stop(Server*);
