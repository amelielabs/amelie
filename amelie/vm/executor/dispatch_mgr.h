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
	List          ltrs;
	int           ltrs_count;
	Complete      complete;
	Dtr*          dtr;
	ReqCache      cache_req;
	LtrCache      cache_ltr;
	DispatchCache cache;
};

hot always_inline static inline Dispatch*
dispatch_mgr_at(DispatchMgr* self, int order)
{
	assert(order < self->list_count);
	return ((Dispatch**)self->list.start)[order];
}

static inline bool
dispatch_mgr_is_first(DispatchMgr* self)
{
	return self->list_count == 0;
}

static inline void
dispatch_mgr_init(DispatchMgr* self, Dtr* dtr)
{
	self->list_count = 0;
	self->ltrs_count = 0;
	self->dtr        = dtr;
	buf_init(&self->list);
	list_init(&self->ltrs);
	req_cache_init(&self->cache_req);
	ltr_cache_init(&self->cache_ltr);
	dispatch_cache_init(&self->cache);
	complete_init(&self->complete);
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

	list_foreach_safe(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		ltr_cache_push(&self->cache_ltr, ltr);
	}
	list_init(&self->ltrs);
	self->ltrs_count = 0;

	complete_reset(&self->complete);
}

static inline void
dispatch_mgr_free(DispatchMgr* self)
{
	dispatch_mgr_reset(self);
	buf_free(&self->list);
	dispatch_cache_free(&self->cache);
	ltr_cache_free(&self->cache_ltr);
	req_cache_free(&self->cache_req);
}

hot static inline void
dispatch_mgr_close(DispatchMgr* self)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->closed)
			continue;
		track_send(&ltr->part->track, &ltr->queue_close);
		ltr->closed = true;
	}
}

hot static inline Ltr*
dispatch_mgr_find(DispatchMgr* self, Part* part)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->part == part)
			return ltr;
	}
	return NULL;
}

hot static inline bool
dispatch_mgr_overlaps(DispatchMgr* self, DispatchMgr* with)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (dispatch_mgr_find(with, ltr->part))
			return true;
	}
	return false;
}

hot static inline bool
dispatch_mgr_overlaps_dispatch(DispatchMgr* self, Dispatch* with)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (dispatch_find(with, ltr->part))
			return true;
	}
	return false;
}

hot static inline void
dispatch_mgr_snapshot(DispatchMgr* self, Access* access)
{
	// create ltr for each partition from access
	for (auto i = 0; i < access->list_count; i++)
	{
		auto ac = access_at(access, i);
		if (ac->lock == LOCK_CALL)
			continue;
		auto table = table_of(ac->rel);
		list_foreach(&engine_main(&table->engine)->list)
		{
			auto part = list_at(Part, link);
			auto ltr = ltr_create(&self->cache_ltr, part, self->dtr, &self->complete);
			list_append(&self->ltrs, &ltr->link);
			self->ltrs_count++;
		}
	}
}

hot static inline void
dispatch_mgr_send(DispatchMgr* self, Dispatch* dispatch)
{
	// handle snapshot by starting execution of all prepared ltrs
	// on first send
	if (dispatch_mgr_is_first(self) && self->ltrs_count > 0)
	{
		list_foreach(&self->ltrs)
		{
			auto ltr   = list_at(Ltr, link);
			auto track = &ltr->part->track;
			ltr->consensus = track->consensus;
			track_send(track, &ltr->msg);
		}
	}

	// add dispatch
	dispatch->order = self->list_count;

	buf_write(&self->list, &dispatch, sizeof(Dispatch**));
	self->list_count++;

	// send dispatch requests
	list_foreach(&dispatch->list)
	{
		auto req   = list_at(Req, link);
		auto track = &req->part->track;
		// find or create transaction
		auto ltr = dispatch_mgr_find(self, req->part);
		if (! ltr)
		{
			assert(self->list_count == 1);
			ltr = ltr_create(&self->cache_ltr, req->part, self->dtr, &self->complete);
			ltr->consensus = track->consensus;
			list_append(&self->ltrs, &ltr->link);
			self->ltrs_count++;
			track_send(track, &ltr->msg);
		}
		ltr->closed = dispatch->close;
		req->ltr = ltr;
		track_send(track, &req->msg);
	}

	// send early close requests
	if (dispatch->close)
		dispatch_mgr_close(self);
}

hot static inline Buf*
dispatch_mgr_complete(DispatchMgr* self, bool* write)
{
	*write = false;
	if (! self->ltrs_count)
		return NULL;

	// make sure all transactions complete execution
	dispatch_mgr_close(self);

	// wait for group completion
	complete_wait(&self->complete);

	// sync metrics
	Buf* error = NULL;
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->error && !error)
			error = ltr->error;
		auto tr = ltr->tr;
		if (! tr)
			continue;

		// is writing transaction
		if (! tr_read_only(tr))
			*write = true;
	}
	return error;
}
