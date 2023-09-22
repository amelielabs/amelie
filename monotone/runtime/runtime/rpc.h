#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Rpc Rpc;

struct Rpc
{
	int        id;
	int        argc;
	intptr_t*  argv;
	int        rc;
	Error      error;
	Channel*   channel;
	Condition* cond;
	List       link;
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
	condition_signal(rpc->cond);
}

hot static inline void
rpc_execute(Buf* buf,
            void (*callback)(Rpc*, void*),
            void  *callback_arg)
{
	auto rpc = rpc_of(buf);
	Exception e;
	if (try(&e))
		callback(rpc, callback_arg);
	if (unlikely(catch(&e)))
		rpc->error = mn_self()->error;
	rpc_done(rpc);
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
	Rpc rpc =
	{
		.id      = id,
		.argc    = argc,
		.argv    = argv,
		.rc      = 0,
		.channel = channel,
		.cond    = condition_create()
	};
	rpc.error.code = ERROR_NONE;
	list_init(&rpc.link);
	auto rpc_ptr = &rpc;

	// do rpc call and wait for completion
	auto buf = msg_create(id);
	buf_write(buf, &rpc_ptr, sizeof(void**));
	msg_end(buf);
	channel_write(channel, buf);

	condition_wait(rpc.cond, -1);
	condition_free(rpc.cond);

	if (unlikely(rpc.error.code != ERROR_NONE))
	{
		mn_self()->error = rpc.error;
		rethrow();
	}

	return rpc.rc;
}
