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
	uint64_t   lsn_last;
	Uuid*      id;
	char       id_sz[UUID_SZ];
	WalSlot*   wal_slot;
	WalCursor  wal_cursor;
	Wal*       wal;
	Condition* on_complete;
	Task       task;
};

void pusher_init(Pusher*, Wal*);
void pusher_free(Pusher*);
void pusher_start(Pusher*, Client*, WalSlot*, Uuid*);
void pusher_stop(Pusher*);
