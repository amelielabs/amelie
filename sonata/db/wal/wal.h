#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Wal Wal;

struct Wal
{
	Mutex    lock;
	bool     enabled;
	IdMgr    list;
	List     slots;
	int      slots_count;
	WalFile* current;
};

void wal_init(Wal*);
void wal_free(Wal*);
void wal_open(Wal*);
void wal_rotate(Wal*);
void wal_gc(Wal*, uint64_t);
void wal_enable(Wal*, bool);
void wal_write(Wal*, WalBatch*);
void wal_add(Wal*, WalSlot*);
void wal_del(Wal*, WalSlot*);
void wal_attach(Wal*, WalSlot*);
void wal_detach(Wal*, WalSlot*);
bool wal_in_range(Wal*, uint64_t);
Buf* wal_show(Wal*);
