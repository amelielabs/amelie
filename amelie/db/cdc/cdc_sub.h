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

typedef struct CdcSub CdcSub;

struct CdcSub
{
	Event*   event;
	uint64_t lsn;
	bool     active;
	List     link;
};

static inline void
cdc_sub_init(CdcSub* self, Event* event, uint64_t lsn)
{
	self->event  = event;
	self->lsn    = lsn;
	self->active = false;
	list_init(&self->link);
}

static inline void
cdc_sub_signal(CdcSub* self)
{
	event_signal(self->event);
}
