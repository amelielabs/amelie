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

typedef struct CdcWaiter CdcWaiter;

struct CdcWaiter
{
	Event event;
	List  link;
};

static inline void
cdc_waiter_init(CdcWaiter* self)
{
	event_init(&self->event);
	event_attach(&self->event);
	list_init(&self->link);
}
