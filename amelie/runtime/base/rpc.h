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
	Msg       msg;
	int       argc;
	intptr_t* argv;
	int       rc;
	Error*    error;
	Event     on_complete;
	List      link;
};

always_inline hot static inline Rpc*
rpc_of(Msg* msg)
{
	return (Rpc*)msg;
}

always_inline hot static inline intptr_t
rpc_arg(Rpc* rpc, int pos)
{
	return rpc->argv[pos];
}

always_inline hot static inline void*
rpc_arg_ptr(Rpc* rpc, int pos)
{
	return (void*)rpc->argv[pos];
}

always_inline hot static inline void
rpc_done(Rpc* rpc)
{
	event_signal(&rpc->on_complete);
}

hot static inline void
rpc_execute(Rpc* self,
            void (*callback)(Rpc*, void*),
            void  *callback_arg)
{
	if (error_catch( callback(self, callback_arg) ))
		*self->error = am_self()->error;
	rpc_done(self);
}

static inline int
rpc(Task* dest, MsgId id, int argc, ...)
{
	// prepare arguments
	intptr_t argv[argc];
	va_list args;
	va_start(args, argc);
	int i = 0;
	for (; i < argc; i++)
		argv[i] = va_arg(args, intptr_t);
	va_end(args);

	// prepare the rpc object
	auto error = &am_self()->error;
	error->code = ERROR_NONE;
	Rpc rpc =
	{
		.argc  = argc,
		.argv  = argv,
		.rc    = 0,
		.error = error,
	};
	msg_init(&rpc.msg, id);
	list_init(&rpc.link);
	event_init_bus(&rpc.on_complete, &am_task->bus);

	// do the call and wait for completion
	task_send(dest, &rpc.msg);

	cancel_pause();
	event_wait(&rpc.on_complete, -1);
	cancel_resume();

	if (unlikely(error->code != ERROR_NONE))
		rethrow();

	return rpc.rc;
}
