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

typedef struct DispatchMgr DispatchMgr;
typedef struct Dtr         Dtr;

struct DispatchMgr
{
	List          list;
	int           list_count;
	DispatchCache cache;
	ReqCache      cache_req;
	Buf           ctrs;
	int           ctrs_count;
	CoreMgr*      core_mgr;
	Complete      complete;
	bool          is_write;
	Msg           close;
	Dtr*          dtr;
};

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
	self->list_count = 0;
	self->ctrs_count = 0;
	self->core_mgr   = core_mgr;
	self->dtr        = dtr;
	self->is_write   = false;
	list_init(&self->list);
	dispatch_cache_init(&self->cache);
	req_cache_init(&self->cache_req);
	buf_init(&self->ctrs);
	msg_init(&self->close, MSG_STOP);
	complete_init(&self->complete);
}

static inline void
dispatch_mgr_reset(DispatchMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto dispatch = list_at(Dispatch, link);
		dispatch_cache_push(&self->cache, dispatch, &self->cache_req);
	}
	list_init(&self->list);
	self->list_count = 0;

	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_reset(ctr);
	}
	self->is_write = false;
	complete_reset(&self->complete);
}

static inline void
dispatch_mgr_free_ctrs(DispatchMgr* self)
{
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
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
	buf_free(&self->ctrs);
	dispatch_cache_free(&self->cache);
	req_cache_free(&self->cache_req);
}

static inline void
dispatch_mgr_prepare(DispatchMgr* self)
{
	// prepare a list of core transactions on first call
	auto cores_count = self->core_mgr->cores_count;
	if (likely(self->ctrs_count == cores_count))
		return;
	dispatch_mgr_free_ctrs(self);
	buf_reserve(&self->ctrs, sizeof(Ctr) * self->core_mgr->cores_count);
	self->ctrs_count = cores_count;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_init(ctr, self->dtr, self->core_mgr->cores[i], &self->complete);
	}
}

hot static inline void
dispatch_mgr_snapshot(DispatchMgr* self)
{
	// start transactions on each core
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr->state = CTR_ACTIVE;
	}
}

hot static inline int
dispatch_mgr_close(DispatchMgr* self)
{
	int active = 0;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		if (ctr->state == CTR_NONE)
			continue;
		active++;
		if (ctr->queue_close)
			continue;
		ring_write(&ctr->queue, &self->close);
		ctr->queue_close = true;
	}
	return active;
}

hot static inline void
dispatch_mgr_send(DispatchMgr* self, Dispatch* dispatch)
{
	// add dispatch
	list_append(&self->list, &dispatch->link);
	self->list_count++;

	// send requests
	list_foreach(&dispatch->list)
	{
		auto req = list_at(Req, link);

		// activate transaction for the core
		auto ctr = dispatch_mgr_ctr(self, req->core->order);
		if (ctr->state == CTR_NONE)
			ctr->state = CTR_ACTIVE;

		// add request to the core transaction queue
		ctr->queue_close = dispatch->close;
		ring_write(&ctr->queue, &req->msg);
	}

	// finilize still active transactions on the last send
	if (dispatch->close)
		dispatch_mgr_close(self);
}

hot static inline Buf*
dispatch_mgr_shutdown(DispatchMgr* self)
{
	// make sure all transactions complete execution
	auto active = dispatch_mgr_close(self);
	if (! active)
		return NULL;

	// wait for group completion
	complete_wait(&self->complete);

	// collect errors
	Buf* error = NULL;
	bool is_write = false;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		if (ctr->state == CTR_NONE)
			continue;
		if (ctr->error && !error)
			error = ctr->error;
		if (ctr->tr && !tr_read_only(ctr->tr))
			is_write = true;
		ctr->state = CTR_COMPLETE;
	}
	self->is_write = is_write;
	return error;
}
