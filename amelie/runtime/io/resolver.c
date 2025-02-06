
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

#include <amelie_runtime.h>
#include <amelie_io.h>

static void
resolver_main(void* arg)
{
	unused(arg);
	for (bool active = true; active;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto rpc = rpc_of(buf);
		buf_free(buf);
		switch (rpc->id) {
		case RPC_STOP:
			active = false;
			break;
		case RPC_RESOLVE:
		{
			const char       *addr    = rpc_arg_ptr(rpc, 0);
			const char       *service = rpc_arg_ptr(rpc, 1);
			struct addrinfo  *hints   = rpc_arg_ptr(rpc, 2);
			struct addrinfo **result  = rpc_arg_ptr(rpc, 3);
			rpc->rc = socket_getaddrinfo(addr, service, hints, result);
			break;
		}
		}
		rpc_done(rpc);
	}
}

void
resolver_init(Resolver* self)
{
	task_init(&self->task);
}

void
resolver_start(Resolver* self)
{
	task_create(&self->task, "resolver", resolver_main, self);
}

void
resolver_stop(Resolver* self)
{
	if (task_active(&self->task))
		rpc(&self->task.channel, RPC_STOP, 0);
	task_wait(&self->task);
	task_free(&self->task);
}

void
resolve(Resolver*         self,
        const char*       addr,
        int               port,
        struct addrinfo** result)
{
	char service[16];
	snprintf(service, sizeof(service), "%d", port);

	struct addrinfo  hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags    = AI_PASSIVE;
	hints.ai_protocol = IPPROTO_TCP;

	int rc;
	rc = rpc(&self->task.channel, RPC_RESOLVE, 4, addr, service, &hints, result);
	if (rc == -1 || *result == NULL)
		error("failed to resolve %s:%d", addr, port);
}
