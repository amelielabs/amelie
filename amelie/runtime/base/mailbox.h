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

typedef struct Mailbox Mailbox;

struct Mailbox
{
	List  list;
	Event event;
};

static inline void
mailbox_init(Mailbox* self)
{
	list_init(&self->list);
	event_init(&self->event);
}

static inline void
mailbox_append(Mailbox* self, Msg* msg)
{
	list_append(&self->list, &msg->link);
}

static inline Msg*
mailbox_pop(Mailbox* self, Coroutine* coro)
{
	while (list_empty(&self->list))
		wait_event(&self->event, coro);
	return container_of(list_pop(&self->list), Msg, link);
}
