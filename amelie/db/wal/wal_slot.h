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
	bool       added;
	Condition* on_write;
	List       link;
};

static inline void
wal_slot_init(WalSlot* self)
{
	self->lsn      = 0;
	self->added    = false;
	self->on_write = NULL;
	list_init(&self->link);
}

static inline void
wal_slot_set(WalSlot* self, uint64_t lsn)
{
	atomic_u64_set(&self->lsn, lsn);
}

static inline void
wal_slot_wait(WalSlot* self)
{
	assert(self->on_write);
	condition_wait(self->on_write, -1);
}

static inline void
wal_slot_signal(WalSlot* self, uint64_t lsn)
{
	if (! self->on_write)
		return;
	if (lsn > atomic_u64_of(&self->lsn))
		condition_signal(self->on_write);
}
