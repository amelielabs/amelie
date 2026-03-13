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

typedef struct WalSlot WalSlot;

struct WalSlot
{
	atomic_u64 lsn;
	bool       active;
	List       link;
};

static inline void
wal_slot_init(WalSlot* self)
{
	self->lsn    = 0;
	self->active = false;
	list_init(&self->link);
}

static inline void
wal_slot_set(WalSlot* self, uint64_t lsn)
{
	atomic_u64_set(&self->lsn, lsn);
}

void wal_attach(Wal*, WalSlot*);
void wal_detach(Wal*, WalSlot*);
void wal_snapshot(Wal*, WalSlot*, Buf*);
int  wal_slots(Wal*, uint64_t*);
