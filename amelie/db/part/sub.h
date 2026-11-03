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

typedef struct Sub Sub;

struct Sub
{
	bool  wait;
	bool  cancel;
	Event event;
	List  link;
};

static inline void
sub_init(Sub* self)
{
	self->wait   = false;
	self->cancel = false;
	event_init(&self->event);
	event_attach(&self->event);
	list_init(&self->link);
}

static inline void
sub_signal(Sub* self)
{
	event_signal(&self->event);
}
