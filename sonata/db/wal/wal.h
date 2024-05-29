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
	IdMgr    list;
	List     slots;
	int      slots_count;
	WalFile* current;
};

void wal_init(Wal*);
void wal_free(Wal*);
void wal_open(Wal*);
void wal_rotate(Wal*, uint64_t);
void wal_gc(Wal*, uint64_t);
bool wal_write(Wal*, WalBatch*);
void wal_add(Wal*, WalSlot*);
void wal_del(Wal*, WalSlot*);
void wal_attach_(Wal*, WalSlot*);
void wal_detach_(Wal*, WalSlot*);
void wal_show(Wal*, Buf*);
