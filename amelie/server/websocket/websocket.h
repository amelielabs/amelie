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

typedef struct Websocket Websocket;

struct Websocket
{
	Iov     iov;
	Client* client;
	bool    client_mode;
	Ws      frame;
	Str     protocol;
};

void websocket_init(Websocket*);
void websocket_free(Websocket*);
void websocket_set(Websocket*, Str*, Client*, bool);
void websocket_connect(Websocket*);
void websocket_accept(Websocket*);
void websocket_send(Websocket*, int, struct iovec*, int, uint64_t);
bool websocket_recv(Websocket*, uint8_t*, int);
void websocket_recv_data(Websocket*, Buf*);
