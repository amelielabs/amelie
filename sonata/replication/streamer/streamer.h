#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Streamer Streamer;

struct Streamer
{
	Client*    client;
	Http       http;
	uint64_t   lsn;
	uint64_t   lsn_last;
	Uuid*      id;
	WalSlot*   wal_slot;
	WalCursor  wal_cursor;
	Wal*       wal;
	Condition* on_complete;
	Task       task;
};

void streamer_init(Streamer*, Wal*);
void streamer_free(Streamer*);
void streamer_start(Streamer*, Client*, WalSlot*, Uuid*);
void streamer_stop(Streamer*);
