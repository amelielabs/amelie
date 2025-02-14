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

typedef struct Wal Wal;

struct Wal
{
	Mutex    lock;
	IdMgr    list;
	List     slots;
	int      slots_count;
	WalFile* current;
};

void wal_init(Wal*);
void wal_free(Wal*);
void wal_open(Wal*);
void wal_close(Wal*);
void wal_gc(Wal*, uint64_t);
void wal_write(Wal*, WalBatch*);
void wal_sync(Wal*);
void wal_add(Wal*, WalSlot*);
void wal_del(Wal*, WalSlot*);
void wal_attach(Wal*, WalSlot*);
void wal_detach(Wal*, WalSlot*);
void wal_snapshot(Wal*, WalSlot*, Buf*);
bool wal_in_range(Wal*, uint64_t);
Buf* wal_status(Wal*);
