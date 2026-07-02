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

typedef struct Player Player;

struct Player
{
	Msg         msg;
	Msg         msg_stop;
	Request     req;
	Query       query;
	Mailbox     queue;
	PlayerSync* sync;
	Buf         buf;
	Frontend*   fe;
	void*       arg;
};

void player_init(Player*, PlayerSync*, Frontend*);
void player_free(Player*);
void player_main(Player*);

static inline void
player_forward(Player* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}
