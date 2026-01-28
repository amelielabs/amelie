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

typedef struct RpcSet RpcSet;

struct RpcSet
{
	Buf set;
	int set_count;
};

static inline Rpc*
rpc_set_at(RpcSet* self, int order)
{
	return &((Rpc*)self->set.start)[order];
}

static inline void
rpc_set_init(RpcSet* self)
{
	self->set_count = 0;
	buf_init(&self->set);
}

static inline void
rpc_set_reset(RpcSet* self)
{
	for (auto at = 0; at < self->set_count; at++)
		rpc_free(rpc_set_at(self, at));
	self->set_count = 0;
	buf_reset(&self->set);
}

static inline void
rpc_set_free(RpcSet* self)
{
	rpc_set_reset(self);
	buf_free(&self->set);
}

static inline void
rpc_set_prepare(RpcSet* self, int count)
{
	buf_reserve(&self->set, sizeof(Rpc) * count);
	self->set_count = count;
}

static inline Rpc*
rpc_set_add(RpcSet* self, int order, MsgId id, void* arg)
{
	auto rpc = rpc_set_at(self, order);
	rpc_init(rpc, id, arg);
	return rpc;
}

static inline void
rpc_set_wait(RpcSet* self)
{
	// collect responses
	Error* error = NULL;
	for (auto at = 0; at < self->set_count; at++)
	{
		auto rpc = rpc_set_at(self, at);
		rpc_recv(rpc);
		if (rpc->error.code != ERROR_NONE && !error)
			error = &rpc->error;
	}

	// rethrow error
	if (unlikely(error))
	{
		error_copy(&am_self()->error, error);
		rethrow();
	}
}
