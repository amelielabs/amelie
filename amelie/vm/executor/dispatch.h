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
typedef struct Dtr          Dtr;

struct DispatchStep
{
	ReqList list;
};

struct Dispatch
{
	Buf      steps;
	int      steps_count;
	int      steps_complete;
	Ctr*     cores;
	CoreMgr* core_mgr;
	ReqCache req_cache;
};

hot always_inline static inline DispatchStep*
dispatch_at(Dispatch* self, int step)
{
	auto step_offset = sizeof(DispatchStep) * step;
	return (DispatchStep*)(self->steps.start + step_offset);
}

hot always_inline static inline Ctr*
dispatch_at_core(Dispatch* self, int core)
{
	return &self->cores[core];
}

static inline void
dispatch_init(Dispatch* self, CoreMgr* core_mgr)
{
	self->steps_count    = 0;
	self->steps_complete = 0;
	self->cores          = NULL;
	self->core_mgr       = core_mgr;
	buf_init(&self->steps);
	req_cache_init(&self->req_cache);
}

static inline void
dispatch_reset(Dispatch* self)
{
	for (auto i = 0; i < self->steps_count; i++)
	{
		auto step = dispatch_at(self, i);
		req_cache_push_list(&self->req_cache, &step->list);
	}
	if (self->cores)
	{
		for (auto i = 0; i < self->core_mgr->cores_count; i++)
		{
			auto ctr = dispatch_at_core(self, i);
			ctr_reset(ctr);
		}
	}
	buf_reset(&self->steps);
	self->steps_complete = 0;
	self->steps_count    = 0;
}

static inline void
dispatch_free(Dispatch* self)
{
	assert(! self->steps_count);
	if (self->cores)
	{
		for (auto i = 0; i < self->core_mgr->cores_count; i++)
		{
			auto ctr = dispatch_at_core(self, i);
			ctr_detach(ctr);
			ctr_free(ctr);
		}
		am_free(self->cores);
	}
	buf_free(&self->steps);
	req_cache_free(&self->req_cache);
}

static inline void
dispatch_create(Dispatch* self, Dtr* dtr, int steps)
{
	// prepare a list of core transactions on first call
	auto cores = self->core_mgr->cores_count;
	if (unlikely(! self->cores))
	{
		self->cores = am_malloc(sizeof(Ctr) * cores);
		for (auto i = 0; i < cores; i++)
		{
			auto ctr = dispatch_at_core(self, i);
			ctr_init(ctr, dtr, self->core_mgr->cores[i]);
			ctr_attach(ctr);
		}
	}

	// create an array of dispatch steps
	auto size = sizeof(DispatchStep) * steps;
	buf_reserve(&self->steps, size);
	memset(self->steps.start, 0, size);
	self->steps_count = steps;
	for (auto i = 0; i < self->steps_count; i++)
	{
		auto step = dispatch_at(self, i);
		req_list_init(&step->list);
	}
}

hot static inline void
dispatch_add(Dispatch* self, int order, Req* req)
{
	// add request to the dispatch step
	auto step = dispatch_at(self, order);
	req_list_add(&step->list, req);

	// activate transaction for the core
	auto ctr = &self->cores[req->core->order];
	if (ctr->state == CTR_NONE)
		ctr->state = CTR_ACTIVE;

	// add request to the core transaction queue
	auto close = (order == (self->steps_count - 1));
	req_queue_send(&ctr->queue, req, close);
}

hot static inline void
dispatch_add_snapshot(Dispatch* self)
{
	// start transactions for all cores
	for (auto i = 0; i < self->core_mgr->cores_count; i++)
	{
		auto ctr = dispatch_at_core(self, i);
		ctr->state = CTR_ACTIVE;
	}
}

hot static inline void
dispatch_wait(Dispatch* self, int step)
{
	// wait for all requests completion
	auto error = (Req*)NULL;
	list_foreach(&dispatch_at(self, step)->list.list)
	{
		auto req = list_at(Req, link);
		event_wait(&req->complete, -1);
		if (req->error && !error)
			error = req;
	}
	self->steps_complete = step;

	if (error)
		msg_error_rethrow(error->error);
}

hot static inline void
dispatch_close(Dispatch* self)
{
	// finilize core transactions
	for (auto i = 0; i < self->core_mgr->cores_count; i++)
	{
		auto ctr = dispatch_at_core(self, i);
		if (ctr->state == CTR_NONE)
			continue;
		if (ctr->queue.close)
			continue;
		req_queue_close(&ctr->queue);
	}
}

hot static inline void
dispatch_shutdown(Dispatch* self)
{
	// not all steps were completed, force close active
	// transactions
	if (self->steps_complete != (self->steps_count - 1))
		dispatch_close(self);

	// make sure all transactions complete execution
	for (auto order = 0; order < self->core_mgr->cores_count; order++)
	{
		auto ctr = &self->cores[order];
		if (ctr->state == CTR_NONE)
			continue;
		event_wait(&ctr->complete, -1);
		ctr->state = CTR_COMPLETE;
	}
}
