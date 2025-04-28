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

typedef struct Dispatch    Dispatch;
typedef struct DispatchMgr DispatchMgr;
typedef struct Dtr         Dtr;

struct Dispatch
{
	int count;
	int offset;
};

struct DispatchMgr
{
	Buf      list;
	int      list_count;
	int      list_processed;
	Buf      reqs;
	int      reqs_count;
	ReqCache req_cache;
	Buf      ctrs;
	int      ctrs_count;
	CoreMgr* core_mgr;
	Dtr*     dtr;
};

always_inline static inline Dispatch*
dispatch_mgr_dispatch(DispatchMgr* self, int order)
{
	assert(order < self->list_count);
	return &((Dispatch*)self->list.start)[order];
}

always_inline static inline Req*
dispatch_mgr_req(DispatchMgr* self, int order)
{
	assert(order < self->reqs_count);
	return ((Req**)self->reqs.start)[order];
}

hot always_inline static inline Ctr*
dispatch_mgr_ctr(DispatchMgr* self, int order)
{
	assert(order < self->ctrs_count);
	return &((Ctr*)self->ctrs.start)[order];
}

static inline bool
dispatch_mgr_is_first(DispatchMgr* self)
{
	return self->list_count == 0;
}

static inline void
dispatch_mgr_init(DispatchMgr* self, CoreMgr* core_mgr, Dtr* dtr)
{
	self->list_count     = 0;
	self->list_processed = 0;
	self->reqs_count     = 0;
	self->ctrs_count     = 0;
	self->core_mgr       = core_mgr;
	self->dtr            = dtr;
	buf_init(&self->list);
	buf_init(&self->reqs);
	buf_init(&self->ctrs);
	req_cache_init(&self->req_cache);
}

static inline void
dispatch_mgr_reset(DispatchMgr* self)
{
	self->list_count     = 0;
	self->list_processed = 0;
	buf_reset(&self->list);

	for (auto i = 0; i < self->reqs_count; i++)
	{
		auto req = dispatch_mgr_req(self, i);
		req_cache_push(&self->req_cache, req);
	}
	self->reqs_count = 0;
	buf_reset(&self->reqs);

	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_reset(ctr);
	}
}

static inline void
dispatch_mgr_free_ctrs(DispatchMgr* self)
{
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_detach(ctr);
		ctr_free(ctr);
	}
	self->ctrs_count = 0;
	buf_reset(&self->ctrs);
}

static inline void
dispatch_mgr_free(DispatchMgr* self)
{
	dispatch_mgr_reset(self);
	dispatch_mgr_free_ctrs(self);
	buf_free(&self->list);
	buf_free(&self->reqs);
	buf_free(&self->ctrs);
	req_cache_free(&self->req_cache);
}

static inline void
dispatch_mgr_prepare(DispatchMgr* self)
{
	// prepare a list of core transactions on first call
	if (likely(self->ctrs_count == self->core_mgr->cores_count))
		return;
	dispatch_mgr_free_ctrs(self);
	buf_reserve(&self->ctrs, sizeof(Ctr) * self->core_mgr->cores_count);
	self->ctrs_count = self->core_mgr->cores_count;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_init(ctr, self->dtr, self->core_mgr->cores[i]);
		ctr_attach(ctr);
	}
}

hot static inline void
dispatch_mgr_snapshot(DispatchMgr* self)
{
	// start transactions for all cores
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr->state = CTR_ACTIVE;
	}
}

hot static inline void
dispatch_mgr_send(DispatchMgr* self, ReqList* list, bool close)
{
	// create new dispatch
	auto dispatch = (Dispatch*)buf_claim(&self->list, sizeof(Dispatch));
	dispatch->count  = list->list_count;
	dispatch->offset = self->reqs_count;
	self->list_count++;

	// add requests to the dispatch
	list_foreach_safe(&list->list)
	{
		auto req = list_at(Req, link);
		req_list_del(list, req);
		buf_write(&self->reqs, &req, sizeof(Req*));
		self->reqs_count++;

		// activate transaction for the core
		auto ctr = dispatch_mgr_ctr(self, req->core->order);
		if (ctr->state == CTR_NONE)
			ctr->state = CTR_ACTIVE;

		// add request to the core transaction queue
		req_queue_send(&ctr->queue, req, close);
	}
}

hot static inline void
dispatch_mgr_recv(DispatchMgr* self)
{
	if (self->list_count == self->list_processed)
		return;

	// wait for all requests completion for the last dispatch
	auto error = (Req*)NULL;
	auto dispatch = dispatch_mgr_dispatch(self, self->list_processed);
	for (auto i = 0; i < dispatch->count; i++)
	{
		auto req = dispatch_mgr_req(self, dispatch->offset + i);
		event_wait(&req->complete, -1);
		if (req->error && !error)
			error = req;
	}
	self->list_processed++;

	if (error)
		msg_error_rethrow(error->error);
}

hot static inline void
dispatch_mgr_close(DispatchMgr* self)
{
	// finilize core transactions
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		if (ctr->state == CTR_NONE)
			continue;
		if (ctr->queue.close)
			continue;
		req_queue_close(&ctr->queue);
	}
}

hot static inline void
dispatch_mgr_shutdown(DispatchMgr* self)
{
	// not all steps were completed, force close active
	// transactions
	if (self->list_count != self->list_processed)
		dispatch_mgr_close(self);

	// make sure all transactions complete execution
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		if (ctr->state == CTR_NONE)
			continue;
		event_wait(&ctr->complete, -1);
		ctr->state = CTR_COMPLETE;
	}
}
