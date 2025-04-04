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

typedef struct DispatchStep DispatchStep;
typedef struct Dispatch     Dispatch;

struct DispatchStep
{
	atomic_u32 complete;
	atomic_u32 errors;
	ReqList    list;
};

struct Dispatch
{
	Buf      steps_data;
	int      steps;
	int      steps_recv;
	int      routes;
	Event    on_complete;
	ReqCache req_cache;
};

hot static inline DispatchStep*
dispatch_at(Dispatch* self, int order)
{
	return &((DispatchStep*)self->steps_data.start)[order];
}

static inline void
dispatch_init(Dispatch* self)
{
	self->steps      = 0;
	self->steps_recv = 0;
	self->routes     = 0;
	buf_init(&self->steps_data);
	event_init(&self->on_complete);
	req_cache_init(&self->req_cache);
}

static inline void
dispatch_reset(Dispatch* self)
{
	for (auto i = 0; i < self->steps; i++)
	{
		auto step = dispatch_at(self, i);
		req_free_list(&step->list);
	}
	buf_reset(&self->steps_data);
	self->steps_recv = 0;
	self->steps      = 0;
	self->routes     = 0;
}

static inline void
dispatch_free(Dispatch* self)
{
	assert(! self->steps);
	event_detach(&self->on_complete);
	buf_free(&self->steps_data);
	req_cache_free(&self->req_cache);
}

static inline void
dispatch_create(Dispatch* self, int steps)
{
	auto size = sizeof(DispatchStep) * steps;
	buf_reserve(&self->steps_data, size);
	memset(self->steps_data.start, 0, size);

	self->steps = steps;
	for (auto i = 0; i < self->steps; i++)
	{
		auto step = dispatch_at(self, i);
		step->complete = 0;
		step->errors   = 0;
		req_list_init(&step->list);
	}

	self->routes = var_int_of(&config()->backends);

	if (! event_attached(&self->on_complete))
		event_attach(&self->on_complete);
}

hot static inline void
dispatch_add(Dispatch* self, int order, Req* req)
{
	auto step = dispatch_at(self, order);
	req_list_add(&step->list, req);
}

hot static inline void
dispatch_wait_step(Dispatch* self, int order)
{
	auto step = dispatch_at(self, order);
	while (atomic_u32_of(&step->complete) < (uint32_t)step->list.list_count)
		event_wait(&self->on_complete, -1);
	self->steps_recv = order;
}

hot static inline void
dispatch_wait(Dispatch* self, int order)
{
	// read all replies from the reqs in the list
	dispatch_wait_step(self, order);

	// find first req error and rethrow
	auto step = dispatch_at(self, order);
	if (unlikely(step->errors > 0))
	{
		list_foreach(&step->list.list)
		{
			auto req = list_at(Req, link);
			if (req->arg.error)
				msg_error_rethrow(req->arg.error);
		}
	}
}

hot static inline void
dispatch_drain(Dispatch* self)
{
	while (self->steps_recv < self->steps - 1)
		dispatch_wait_step(self, self->steps_recv);
}
