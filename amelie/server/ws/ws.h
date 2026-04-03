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

typedef struct WsFrame WsFrame;

enum
{
	WS_CONTINUATION = 0,
	WS_TEXT         = 1,
	WS_BINARY       = 2,
	WS_CLOSE        = 8,
	WS_PING         = 9,
	WS_PONG         = 10
};

struct WsFrame
{
	bool     fin;
	uint8_t  opcode;
	bool     mask;
	uint32_t mask_key;
	uint64_t size;
};

void ws_send(Client*, WsFrame*, int, bool, uint64_t);
void ws_recv(Client*, WsFrame*);
