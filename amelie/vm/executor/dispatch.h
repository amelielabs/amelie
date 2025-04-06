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
	Ctr*    cores[];
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
	auto step_offset = (sizeof(DispatchStep) +
	                    sizeof(Ctr*) * self->core_mgr->cores_count) * step;
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
	for (auto i = 0; i < self->core_mgr->cores_count; i++)
	{
		auto ctr = dispatch_at_core(self, i);
		ctr_reset(ctr);
	}
	buf_reset(&self->steps);
	self->steps_complete = 0;
	self->steps_count    = 0;
}

static inline void
dispatch_free(Dispatch* self)
{
	assert(! self->steps_count);
	for (auto i = 0; i < self->core_mgr->cores_count; i++)
	{
		auto ctr = dispatch_at_core(self, i);
		req_queue_detach(&ctr->queue);
		ctr_free(ctr);
	}
	if (self->cores)
		am_free(self->cores);
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
			req_queue_attach(&ctr->queue);
		}
	}

	// create an array of dispatch steps
	auto size = (sizeof(DispatchStep) + sizeof(Ctr*) * cores) * steps;
	buf_reserve(&self->steps, size);
	memset(self->steps.start, 0, size);
	self->steps_count = steps;
	for (auto i = 0; i < self->steps_count; i++)
	{
		auto step = dispatch_at(self, i);
		req_list_init(&step->list);
		memset(step->cores, 0, sizeof(Ctr*) * cores);
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
		ctr_begin(ctr);
	step->cores[req->core->order] = ctr;

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
		ctr_begin(ctr);
	}
}

hot static inline void
dispatch_close(Dispatch* self)
{
	// finilize the last step for transactions that were not
	// involved in it but still active
	auto step_last = self->steps_count - 1;
	auto step = dispatch_at(self, step_last);

	auto cores = self->core_mgr->cores_count;
	if (likely(step->list.list_count == cores))
		return;
	for (auto order = 0; order < cores; order++)
	{
		auto ctr = &self->cores[order];
		if (ctr->state == CTR_NONE)
			continue;
		if (step->cores[order])
			continue;
		req_queue_close(&ctr->queue);
	}
}

hot static inline void
dispatch_wait(Dispatch* self, int order)
{
	// read all replies from the reqs in the step
	auto step = dispatch_at(self, order);
	auto step_errors = 0;
	for (auto order = 0; order < self->core_mgr->cores_count; order++)
	{
		auto ctr = step->cores[order];
		if (! ctr)
			continue;
		auto error = req_queue_recv(&ctr->queue);
		if (error)
			step_errors++;
	}

	// mark last completed step
	self->steps_complete = order;

	// rethow first error
	if (unlikely(step_errors > 0))
	{
		list_foreach(&step->list.list)
		{
			auto req = list_at(Req, link);
			if (req->error)
				msg_error_rethrow(req->error);
		}
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
		req_queue_recv_exit(&ctr->queue);
		ctr_complete(ctr);
	}
}
