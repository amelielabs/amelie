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

typedef struct Ipc Ipc;

typedef enum
{
	IPC_EVENT,
	IPC_MSG
} IpcId;

struct Ipc
{
	IpcId      id;
	atomic_u32 refs;
};

static inline void
ipc_init(Ipc* self, IpcId id)
{
	self->id   = id;
	self->refs = 0;
}

static inline bool
ipc_ref(Ipc* self)
{
	return atomic_u32_inc(&self->refs) == 0;
}

static inline void
ipc_unref(Ipc* self)
{
	atomic_u32_set(&self->refs, 0);
}
