#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Pusher Pusher;

struct Pusher
{
	uint64_t   lsn;
	char       id[UUID_SZ];
	Client*    client;
	WalCursor  wal_cursor;
	WalSlot*   wal_slot;
	Wal*       wal;
	Condition* on_complete;
	Task       task;
};

void pusher_init(Pusher*, Wal*, WalSlot*, Uuid*);
void pusher_free(Pusher*);
void pusher_start(Pusher*, Client*);
void pusher_stop(Pusher*);
