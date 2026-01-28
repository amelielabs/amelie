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

typedef struct Rpc Rpc;

struct Rpc
{
	Msg   msg;
	void* arg;
	Error error;
	Event on_complete;
};

static inline Rpc*
rpc_of(Msg* msg)
{
	return (Rpc*)msg;
}

static inline void
rpc_init(Rpc* self, MsgId id, void* arg)
{
	self->arg = arg;
	msg_init(&self->msg, id);
	error_init(&self->error);
	event_init(&self->on_complete);
}

static inline void
rpc_free(Rpc* self)
{
	error_free(&self->error);
}

static inline void
rpc_signal(Rpc* rpc)
{
	event_signal(&rpc->on_complete);
}

static inline void
rpc_send(Rpc* self, Task* task)
{
	event_attach(&self->on_complete);
	task_send(task, &self->msg);
}

static inline void
rpc_recv(Rpc* self)
{
	cancel_pause();
	event_wait(&self->on_complete, -1);
	cancel_resume();
}

static inline void
rpc(Task* task, MsgId id, void* arg)
{
	// do rpc and wait for completion
	Rpc rpc;
	rpc_init(&rpc, id, arg);
	defer(rpc_free, &rpc);
	rpc_send(&rpc, task);
	rpc_recv(&rpc);

	// rethrow error
	if (unlikely(rpc.error.code != ERROR_NONE))
	{
		error_copy(&am_self()->error, &rpc.error);
		rethrow();
	}
}

static inline void
rpc_execute(Rpc* self, void (*main)(Rpc*, void*), void* main_arg)
{
	if (error_catch( main(self, main_arg) ))
		error_copy(&self->error, &am_self()->error);
	rpc_signal(self);
}
