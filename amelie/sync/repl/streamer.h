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

typedef struct Streamer Streamer;

struct Streamer
{
	Websocket  websocket;
	Client*    client;
	atomic_u32 connected;
	uint64_t   lsn;
	WalCursor  wal_cursor;
	WalSlot*   wal_slot;
	Wal*       wal;
	Endpoint*  endpoint;
	Uuid       id_primary;
	Uuid       id_replica;
	Task       task;
	List       link;
};

void streamer_init(Streamer*, Wal*, WalSlot*);
void streamer_free(Streamer*);
void streamer_start(Streamer*, Uuid*, Endpoint*);
void streamer_stop(Streamer*);
