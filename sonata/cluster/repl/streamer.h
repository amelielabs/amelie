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
	uint64_t   lsn;
	WalCursor  wal_cursor;
	WalSlot*   wal_slot;
	Wal*       wal;
	Str*       replica_uri;
	char       replica_id[UUID_SZ];
	Condition* on_complete;
	Task       task;
	List       link;
};

void streamer_init(Streamer*, Wal*, WalSlot*);
void streamer_free(Streamer*);
void streamer_start(Streamer*, Uuid*, Str*);
void streamer_stop(Streamer*);
