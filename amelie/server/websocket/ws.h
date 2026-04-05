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

typedef struct Ws Ws;

enum
{
	WS_CONTINUATION = 0,
	WS_TEXT         = 1,
	WS_BINARY       = 2,
	WS_CLOSE        = 8,
	WS_PING         = 9,
	WS_PONG         = 10
};

struct Ws
{
	bool     fin;
	uint8_t  opcode;
	bool     mask;
	uint32_t mask_key;
	uint64_t size;
	uint8_t  header[14];
	int      header_size;
};

static inline void
ws_init(Ws* self)
{
	memset(self, 0, sizeof(*self));
}

void ws_create(Ws*);
bool ws_recv(Ws*, Client*);
