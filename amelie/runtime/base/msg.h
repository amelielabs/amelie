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

typedef struct Msg    Msg;
typedef struct MsgBuf MsgBuf;

typedef enum
{
	// generic
	MSG_STOP,

	// resolver
	MSG_RESOLVE,

	// storage
	MSG_RECORD,
	MSG_OPEN,

	// commit
	MSG_GTR,

	// compute
	MSG_CLIENT,

	// system
	MSG_SHOW_METRICS
} MsgId;

struct Msg
{
	Ipc   ipc;
	MsgId id;
	List  link;
};

struct MsgBuf
{
	Msg  msg;
	Buf* msg_buf;
};

static inline void
msg_init(Msg* self, MsgId id)
{
	self->id = id;
	ipc_init(&self->ipc, IPC_MSG);
	list_init(&self->link);
}
