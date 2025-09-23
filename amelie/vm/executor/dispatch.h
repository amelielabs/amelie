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

typedef struct Dispatch Dispatch;
typedef struct Dtr      Dtr;

struct Dispatch
{
	Buf      reqs;
	int      reqs_count;
	ReqCache req_cache;
	Buf      ctrs;
	int      ctrs_count;
	CoreMgr* core_mgr;
	bool     write;
	Msg      close;
	Dtr*     dtr;
};

always_inline static inline Req*
dispatch_req(Dispatch* self, int order)
{
	assert(order < self->reqs_count);
	return ((Req**)self->reqs.start)[order];
}

hot always_inline static inline Ctr*
dispatch_ctr(Dispatch* self, int order)
{
	assert(order < self->ctrs_count);
	return &((Ctr*)self->ctrs.start)[order];
}

static inline bool
dispatch_is_first(Dispatch* self)
{
	return self->reqs_count == 0;
}

static inline void
dispatch_init(Dispatch* self, CoreMgr* core_mgr, Dtr* dtr)
{
	self->reqs_count = 0;
	self->ctrs_count = 0;
	self->core_mgr   = core_mgr;
	self->dtr        = dtr;
	self->write      = false;
	buf_init(&self->reqs);
	buf_init(&self->ctrs);
	req_cache_init(&self->req_cache);
	msg_init(&self->close, MSG_STOP);
}

static inline void
dispatch_reset(Dispatch* self)
{
	for (auto i = 0; i < self->reqs_count; i++)
	{
		auto req = dispatch_req(self, i);
		req_cache_push(&self->req_cache, req);
	}
	self->reqs_count = 0;
	buf_reset(&self->reqs);

	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_ctr(self, i);
		ctr_reset(ctr);
	}
	self->write = false;
}

static inline void
dispatch_free_ctrs(Dispatch* self)
{
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_ctr(self, i);
		ctr_free(ctr);
	}
	self->ctrs_count = 0;
	buf_reset(&self->ctrs);
}

static inline void
dispatch_free(Dispatch* self)
{
	dispatch_reset(self);
	dispatch_free_ctrs(self);
	buf_free(&self->reqs);
	buf_free(&self->ctrs);
	req_cache_free(&self->req_cache);
}

static inline void
dispatch_prepare(Dispatch* self)
{
	// prepare a list of core transactions on first call
	if (likely(self->ctrs_count == self->core_mgr->cores_count))
		return;
	dispatch_free_ctrs(self);
	buf_reserve(&self->ctrs, sizeof(Ctr) * self->core_mgr->cores_count);
	self->ctrs_count = self->core_mgr->cores_count;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_ctr(self, i);
		ctr_init(ctr, self->dtr, self->core_mgr->cores[i]);
		ctr_attach(ctr);
	}
}

hot static inline void
dispatch_snapshot(Dispatch* self)
{
	// start transactions for all cores
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_ctr(self, i);
		ctr->state = CTR_ACTIVE;
	}
}

hot static inline void
dispatch_send(Dispatch* self, ReqList* list, bool close)
{
	// add requests to the dispatch
	list_foreach_safe(&list->list)
	{
		auto req = list_at(Req, link);
		req_list_del(list, req);
		buf_write(&self->reqs, &req, sizeof(Req*));
		self->reqs_count++;

		// activate transaction for the core
		auto ctr = dispatch_ctr(self, req->core->order);
		if (ctr->state == CTR_NONE)
			ctr->state = CTR_ACTIVE;

		// add request to the core transaction queue
		ctr->queue_close = close;
		req->close = close;
		ring_write(&ctr->queue, &req->msg);
	}

	// finilize still active transactions on the last send
	if (close)
	{
		for (auto i = 0; i < self->ctrs_count; i++)
		{
			auto ctr = dispatch_ctr(self, i);
			if (ctr->state == CTR_NONE || ctr->queue_close)
				continue;
			ring_write(&ctr->queue, &self->close);
			ctr->queue_close = true;
		}
	}
}

hot static inline Buf*
dispatch_shutdown(Dispatch* self)
{
	// make sure all transactions complete execution
	Buf* error = NULL;
	bool is_write = false;
	for (auto i = 0; i < self->ctrs_count; i++)
	{
		auto ctr = dispatch_ctr(self, i);
		if (ctr->state == CTR_NONE)
			continue;
		if (! ctr->queue_close)
		{
			// force close active transactions
			ring_write(&ctr->queue, &self->close);
			ctr->queue_close = true;
		}
		event_wait(&ctr->complete, -1);

		if (ctr->error && !error)
			error = ctr->error;
		if (ctr->tr && !tr_read_only(ctr->tr))
			is_write = true;
		ctr->state = CTR_COMPLETE;
	}
	self->write = is_write;
	return error;
}
