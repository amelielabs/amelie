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
	int       id;
	int       argc;
	intptr_t* argv;
	int       rc;
	Error*    error;
	Channel*  channel;
	Event     on_complete;
	List      link;
};

always_inline hot static inline Rpc*
rpc_of(Buf* buf)
{
	return *(Rpc**)msg_of(buf)->data;
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

hot static inline int
rpc(Channel* channel, int id, int argc, ...)
{
	// prepare arguments
	intptr_t argv[argc];
	va_list args;
	va_start(args, argc);
	int i = 0;
	for (; i < argc; i++)
		argv[i] = va_arg(args, intptr_t);
	va_end(args);

	// prepare condition
	auto coro  = am_self();
	auto error = &coro->error;
	error->code = ERROR_NONE;
	Rpc rpc =
	{
		.id      = id,
		.argc    = argc,
		.argv    = argv,
		.rc      = 0,
		.error   = error,
		.channel = channel
	};
	list_init(&rpc.link);
	event_init(&rpc.on_complete);
	event_attach(&rpc.on_complete);

	// do rpc call and wait for completion
	auto rpc_ptr = &rpc;
	auto buf = msg_create(id);
	buf_write(buf, &rpc_ptr, sizeof(void**));
	msg_end(buf);
	channel_write(channel, buf);

	cancel_pause();
	event_wait(&rpc.on_complete, -1);
	event_detach(&rpc.on_complete);
	cancel_resume();

	if (unlikely(error->code != ERROR_NONE))
		rethrow();

	return rpc.rc;
}
