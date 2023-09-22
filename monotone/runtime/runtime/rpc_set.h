#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RpcSet RpcSet;

struct RpcSet
{
	List  set;
	int   set_sent;
	int   set_recv;
	int   set_size;
	Rpc*  reserve;
	int   reserve_count;
	int   reserve_used;
	Event parent;
};

static inline void
rpc_set_init(RpcSet* self)
{
	self->reserve       = NULL;
	self->reserve_count = 0;
	self->reserve_used  = 0;
	self->set_sent      = 0;
	self->set_recv      = 0;
	self->set_size      = 0;
	event_init(&self->parent);
	list_init(&self->set);
}

static inline void
rpc_set_free(RpcSet* self)
{
	for (int i = 0; i < self->reserve_count; i++)
	{
		auto rpc = &self->reserve[i];
		if (rpc->cond)
		{
			event_set_parent(&rpc->cond->event, NULL);
			condition_free(rpc->cond);
		}
	}
}

static inline void
rpc_set_reset(RpcSet* self)
{
	self->set_size     = 0;
	self->set_sent     = 0;
	self->set_recv     = 0;
	self->reserve_used = 0;
	list_init(&self->set);
}

static inline void
rpc_set_create(RpcSet* self, int count)
{
	int size = sizeof(Rpc) * count;
	self->reserve_count = count;
	self->reserve = mn_malloc(sizeof(Rpc) * count);
	memset(self->reserve, 0, size);
	for (int i = 0; i < count; i++)
	{
		auto rpc = &self->reserve[i];
		rpc->cond = condition_create();
		event_set_parent(&rpc->cond->event, &self->parent);
		list_init(&rpc->link);
	}
}

static inline void
rpc_set_add(RpcSet*   self, int id, Channel* channel,
            int       argc,
            intptr_t* argv)
{
	assert(self->reserve_used < self->reserve_count);

	auto rpc = &self->reserve[self->reserve_used];
	self->reserve_used++;
	rpc->id      = id;
	rpc->argc    = argc;
	rpc->argv    = argv;
	rpc->channel = channel;
	list_init(&rpc->link);

	list_append(&self->set, &rpc->link);
	self->set_size++;
}

static inline void
rpc_set_execute(RpcSet* self)
{
	list_foreach(&self->set)
	{
		auto rpc = list_at(Rpc, link);
		auto buf = msg_create(rpc->id);
		buf_write(buf, &rpc, sizeof(void**));
		msg_end(buf);
		channel_write(rpc->channel, buf);
		self->set_sent++;
	}
}

static inline void
rpc_set_wait(RpcSet* self)
{
	coroutine_cancel_pause(mn_self());

	while (self->set_recv < self->set_sent)
	{
		event_wait(&self->parent, -1);
		self->set_recv++;
	}

	coroutine_cancel_resume(mn_self());
	assert(self->set_sent == self->set_recv);
}
