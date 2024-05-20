#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Wal Wal;

struct Wal
{
	WalStore wal_store;
	Task     task;
};

void wal_init(Wal*);
void wal_free(Wal*);
void wal_open(Wal*);
void wal_start(Wal*); 
void wal_stop(Wal*); 
void wal_gc(Wal*, uint64_t);
void wal_snapshot(Wal*, WalSnapshot*);
void wal_write(Wal*, LogSet*);
Buf* wal_status(Wal*);
