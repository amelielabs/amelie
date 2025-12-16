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
	Buf           list;
	int           list_count;
	DispatchCache cache;
	ReqCache      cache_req;
	Buf           ctrs;
	int           ctrs_count;
	CoreMgr*      core_mgr;
	bool          is_write;
	uint64_t      tsn_max;
	Dtr*          dtr;
};

hot always_inline static inline Dispatch*
dispatch_mgr_at(DispatchMgr* self, int order)
{
	assert(order < self->list_count);
	return ((Dispatch**)self->list.start)[order];
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
	self->list_count = 0;
	self->ctrs_count = 0;
	self->core_mgr   = core_mgr;
	self->dtr        = dtr;
	self->is_write   = false;
	self->tsn_max    = 0;
	dispatch_cache_init(&self->cache);
	req_cache_init(&self->cache_req);
	buf_init(&self->list);
	buf_init(&self->ctrs);
}

static inline void
dispatch_mgr_reset(DispatchMgr* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto dispatch = dispatch_mgr_at(self, i);
		dispatch_cache_push(&self->cache, dispatch, &self->cache_req);
	}
	buf_reset(&self->list);
	self->list_count = 0;

	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_reset(ctr);
	}
	self->is_write = false;
	self->tsn_max  = 0;
}

static inline void
dispatch_mgr_free(DispatchMgr* self)
{
	dispatch_mgr_reset(self);
	buf_free(&self->list);
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
	buf_reset(&self->ctrs);
	buf_reserve(&self->ctrs, sizeof(Ctr) * self->core_mgr->cores_count);
	self->ctrs_count = cores_count;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		ctr_init(ctr, self->dtr, self->core_mgr->cores[i]);
	}
}

hot static inline void
dispatch_mgr_send(DispatchMgr* self, Dispatch* dispatch)
{
	// add dispatch
	dispatch->order = self->list_count;

	buf_write(&self->list, &dispatch, sizeof(Dispatch**));
	self->list_count++;

	// send requests
	list_foreach(&dispatch->list)
	{
		auto req = list_at(Req, link);

		// activate transaction for the core
		auto ctr = dispatch_mgr_ctr(self, req->core->order);
		ctr->last = dispatch;
		req->ctr  = ctr;

		// send for execution
		core_send(req->core, req);
	}
}

hot static inline void
dispatch_mgr_sync(DispatchMgr* self)
{
	// closing sync logic.
	//
	// add sync requests to the final sync dispatch for all cores
	// that had previous non-returning dispatch.
	//
	auto sync = dispatch_create(&self->cache);
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		if (!ctr->last || ctr->last->returning)
			continue;
		dispatch_add(sync, &self->cache_req, REQ_SYNC, 0,
		             NULL, NULL, ctr->core);
	}
	if (! sync->list_count)
	{
		dispatch_cache_push(&self->cache, sync, &self->cache_req);
		return;
	}

	// prepare, send and wait for completion
	dispatch_set_returning(sync);
	dispatch_prepare(sync);
	dispatch_mgr_send(self, sync);
	dispatch_wait(sync);
}

hot static inline Buf*
dispatch_mgr_shutdown(DispatchMgr* self)
{
	// make sure all transactions complete execution
	dispatch_mgr_sync(self);

	// collect errors
	Buf* error = NULL;
	bool is_write = false;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_mgr_ctr(self, i);
		if (! ctr->last)
			continue;
		if (ctr->error && !error)
			error = ctr->error;

		auto tr = ctr->tr;
		if (! tr)
			continue;

		if (! tr_read_only(ctr->tr))
			is_write = true;

		// sync tsn max for dtr
		if (tr->tsn_max > self->tsn_max)
			self->tsn_max = tr->tsn_max;
	}
	self->is_write = is_write;
	return error;
}
