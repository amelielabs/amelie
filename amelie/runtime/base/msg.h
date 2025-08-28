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

typedef struct Msg Msg;

enum
{
	MSG_CLIENT,
	MSG_NATIVE,

	// rpc
	RPC_START,
	RPC_STOP,
	RPC_RESOLVE,

	RPC_LOCK,
	RPC_UNLOCK,
	RPC_CHECKPOINT,

	RPC_SYNC_USERS,
	RPC_SHOW_METRICS
};

struct Msg
{
	int  id;
	List link;
};

static inline void
msg_init(Msg* self, int id)
{
	self->id = id;
	list_init(&self->link);
}
