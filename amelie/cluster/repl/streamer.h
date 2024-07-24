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
	atomic_u32 connected;
	uint64_t   lsn;
	WalCursor  wal_cursor;
	WalSlot*   wal_slot;
	Wal*       wal;
	Str*       replica_uri;
	char       replica_id[UUID_SZ];
	Task       task;
	List       link;
};

void streamer_init(Streamer*, Wal*, WalSlot*);
void streamer_free(Streamer*);
void streamer_start(Streamer*, Uuid*, Str*);
void streamer_stop(Streamer*);
