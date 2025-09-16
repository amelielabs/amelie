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

typedef struct Msg     Msg;
typedef struct Channel Channel;

typedef enum
{
	// generic
	MSG_STOP,

	// resolver
	MSG_RESOLVE,

	// executor
	MSG_REQ,

	// frontend
	MSG_CLIENT,
	MSG_NATIVE,

	// system
	MSG_LOCK,
	MSG_UNLOCK,
	MSG_CHECKPOINT,
	MSG_SYNC_USERS,
	MSG_SHOW_METRICS
} MsgId;

struct Msg
{
	Ipc   ipc;
	MsgId id;
	List  link;
};

static inline void
msg_init(Msg* self, MsgId id)
{
	self->id = id;
	ipc_init(&self->ipc, IPC_MSG);
	list_init(&self->link);
}
