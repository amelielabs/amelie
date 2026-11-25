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

typedef struct Service Service;
typedef struct Compute Compute;

struct Service
{
	Mailbox  mailbox;
	Request  req;
	void*    session;
	Buf      buf;
	int64_t  worker_id;
	Compute* compute;
};

void service_init(Service*, Compute*);
void service_free(Service*);
void service_start(Service*);
void service_stop(Service*);

static inline void
service_add(Service* self, Msg* msg)
{
	mailbox_append(&self->mailbox, msg);
	event_signal(&self->mailbox.event);
}
