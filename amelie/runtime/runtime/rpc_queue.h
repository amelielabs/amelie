#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct RpcQueue RpcQueue;

struct RpcQueue
{
	List list;
	int  list_count;
};

static inline void
rpc_queue_init(RpcQueue* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
rpc_queue_add(RpcQueue* self, Rpc* rpc)
{
	list_append(&self->list, &rpc->link);
	self->list_count++;
}

static inline Rpc*
rpc_queue_pop(RpcQueue* self)
{
	if (! self->list_count)
		return NULL;
	auto next = list_pop(&self->list);
	self->list_count--;
	return container_of(next, Rpc, link);
}
