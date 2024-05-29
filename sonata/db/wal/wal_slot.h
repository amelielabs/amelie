#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct WalSlot WalSlot;

struct WalSlot
{
	atomic_u64 lsn;
	bool       attached;
	List       link;
};

static inline void
wal_slot_init(WalSlot* self)
{
	self->lsn = 0;
	self->attached = false;
	list_init(&self->link);
}

static inline void
wal_slot_set(WalSlot* self, uint64_t lsn)
{
	atomic_u64_set(&self->lsn, lsn);
}
