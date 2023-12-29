#pragma once

//
// indigo
//
// SQL OLTP database
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
