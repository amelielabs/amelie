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

typedef struct Subscriber Subscriber;

struct Subscriber
{
	Action*      pos;
	Action*      end;
	ServicePage* page;
	Subscriber*  wait_next;
	Event        event;
	List         link;
};

static inline void
subscriber_init(Subscriber* self)
{
	self->pos       = NULL;
	self->end       = NULL;
	self->page      = NULL;
	self->wait_next = NULL;
	event_init(&self->event);
	list_init(&self->link);
}
