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

typedef struct WalSub WalSub;

struct WalSub
{
	Event*   event;
	uint64_t lsn;
	bool     active;
	List     link;
};

static inline void
wal_sub_init(WalSub* self, Event* event, uint64_t lsn)
{
	self->event  = event;
	self->lsn    = lsn;
	self->active = false;
	list_init(&self->link);
}

static inline void
wal_sub_signal(WalSub* self)
{
	event_signal(self->event);
}

void wal_subscribe_notify(Wal*, uint64_t);
void wal_subscribe(Wal*, WalSub*);
void wal_unsubscribe(Wal*, WalSub*);
