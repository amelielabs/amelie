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
	Client*    client;
	atomic_u32 connected;
	uint64_t   lsn;
	WalCursor  wal_cursor;
	WalSlot*   wal_slot;
	Wal*       wal;
	Remote*    remote;
	char       replica_id[UUID_SZ];
	Task       task;
	List       link;
};

void streamer_init(Streamer*, Wal*, WalSlot*);
void streamer_free(Streamer*);
void streamer_start(Streamer*, Uuid*, Remote*);
void streamer_stop(Streamer*);
