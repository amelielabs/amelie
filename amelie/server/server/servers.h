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

typedef struct Servers Servers;

struct Servers
{
	List        list;
	int         list_count;
	ServerEvent on_connect;
	void*       on_connect_arg;
};

void servers_init(Servers*, ServerEvent, void*);
void servers_free(Servers*);
void servers_open(Servers*);
void servers_start(Servers*);
void servers_stop(Servers*);
