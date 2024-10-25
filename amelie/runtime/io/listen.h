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

typedef struct Listen Listen;

struct Listen
{
	Fd      fd;
	Poller* poller;
};

void listen_init(Listen*);
void listen_start(Listen*, int, struct sockaddr*);
void listen_stop(Listen*);
int  listen_accept(Listen*);
