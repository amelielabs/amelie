#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Rpc Rpc;

struct Rpc
{
	int       id;
	int       argc;
	intptr_t* argv;
	int       rc;
	Buf*      buf;
	Task*     dst;
	Task*     src;
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
	channel_write(&rpc->src->channel, rpc->buf);
}

hot static inline void
rpc_execute(Rpc* self,
            void (*callback)(Rpc*, void*),
            void  *callback_arg)
{
	Exception e;
	if (enter(&e))
		callback(self, callback_arg);
	leave(&e);
	if (unlikely(e.triggered))
		self->src->error = am_self->error;
	rpc_done(self);
}

hot static inline int
rpc(Task* dst, int id, int argc, ...)
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
	auto error = &am_self->error;
	error->code = ERROR_NONE;
	Rpc rpc =
	{
		.id   = id,
		.argc = argc,
		.argv = argv,
		.rc   = 0,
		.buf  = NULL,
		.dst  = dst,
		.src  = am_self
	};
	auto rpc_ptr = &rpc;

	// do rpc call and wait for completion
	rpc.buf = msg_begin(id);
	buf_write(rpc.buf, &rpc_ptr, sizeof(void**));
	msg_end(rpc.buf);

	channel_write(&rpc.dst->channel, rpc.buf);
	channel_read(&rpc.src->channel);
	buf_free(rpc.buf);

	if (unlikely(am_self->error.code != ERROR_NONE))
		rethrow();

	return rpc.rc;
}
