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

typedef struct CdcSlot CdcSlot;

struct CdcSlot
{
	atomic_u64 lsn;
	bool       attached;
	List       link;
};

static inline void
cdc_slot_init(CdcSlot* self)
{
	self->lsn = 0;
	self->attached = false;
	list_init(&self->link);
}

static inline void
cdc_slot_set(CdcSlot* self, uint64_t value)
{
	atomic_u64_set(&self->lsn, value);
}
