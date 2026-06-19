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

typedef struct Replay Replay;

struct Replay
{
	Msg         msg;
	Msg         msg_stop;
	Request     req;
	Query       query;
	Mailbox     queue;
	ReplaySync* sync;
	Buf         buf;
	Frontend*   fe;
	void*       arg;
};

void replay_init(Replay*, ReplaySync*, Frontend*);
void replay_free(Replay*);
void replay_main(Replay*);

static inline void
replay_forward(Replay* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}
