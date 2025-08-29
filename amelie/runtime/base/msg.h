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
	// generic
	MSG_STOP,

	// resolver
	MSG_RESOLVE,

	// frontend
	MSG_CLIENT,
	MSG_NATIVE,

	// system
	MSG_LOCK,
	MSG_UNLOCK,
	MSG_CHECKPOINT,
	MSG_SYNC_USERS,
	MSG_SHOW_METRICS
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
