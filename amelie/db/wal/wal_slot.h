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
	bool       added;
	Event      on_write;
	List       link;
};

static inline void
wal_slot_init(WalSlot* self)
{
	self->lsn      = 0;
	self->added    = false;
	event_init(&self->on_write);
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
	event_wait(&self->on_write, -1);
}

static inline void
wal_slot_signal(WalSlot* self, uint64_t lsn)
{
	if (! event_attached(&self->on_write))
		return;
	if (lsn > atomic_u64_of(&self->lsn))
		event_signal(&self->on_write);
}
