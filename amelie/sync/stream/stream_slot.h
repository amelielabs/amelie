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

typedef struct StreamSlot StreamSlot;

struct StreamSlot
{
	atomic_u64 lsn;
	List       link;
};

static inline void
stream_slot_init(StreamSlot* self)
{
	self->lsn = 0;
	list_init(&self->link);
}

static inline void
stream_slot_set(StreamSlot* self, uint64_t value)
{
	atomic_u64_set(&self->lsn, value);
}
