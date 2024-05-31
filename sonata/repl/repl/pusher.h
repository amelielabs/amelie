#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Pusher Pusher;

struct Pusher
{
	Client*    client;
	uint64_t   lsn;
	WalCursor  wal_cursor;
	WalSlot*   wal_slot;
	Wal*       wal;
	Node*      node;
	Condition* on_complete;
	Task       task;
};

void pusher_init(Pusher*, Wal*, WalSlot*, Node*);
void pusher_free(Pusher*);
void pusher_start(Pusher*);
void pusher_stop(Pusher*);
