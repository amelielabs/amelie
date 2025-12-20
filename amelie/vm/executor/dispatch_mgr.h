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
	List          ptrs;
	int           ptrs_count;
	Complete      complete;
	Dtr*          dtr;
	ReqCache      cache_req;
	PtrCache      cache_ptr;
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
	self->ptrs_count = 0;
	self->dtr        = dtr;
	buf_init(&self->list);
	list_init(&self->ptrs);
	req_cache_init(&self->cache_req);
	ptr_cache_init(&self->cache_ptr);
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

	list_foreach_safe(&self->ptrs)
	{
		auto ptr = list_at(Ptr, link);
		ptr_cache_push(&self->cache_ptr, ptr);
	}
	list_init(&self->ptrs);
	self->ptrs_count = 0;

	complete_reset(&self->complete);
}

static inline void
dispatch_mgr_free(DispatchMgr* self)
{
	dispatch_mgr_reset(self);
	buf_free(&self->list);
	dispatch_cache_free(&self->cache);
	ptr_cache_free(&self->cache_ptr);
	req_cache_free(&self->cache_req);
}

hot static inline int
dispatch_mgr_close(DispatchMgr* self)
{
	int active = 0;
	list_foreach(&self->ptrs)
	{
		auto ptr = list_at(Ptr, link);
		if (ptr->closed)
			continue;
		pod_send_backend(ptr->part->pod, &ptr->queue_close);
		ptr->closed = true;
		active++;
	}
	return active;
}

hot static inline Ptr*
dispatch_mgr_find(DispatchMgr* self, Part* part)
{
	list_foreach(&self->ptrs)
	{
		auto ptr = list_at(Ptr, link);
		if (ptr->part == part)
			return ptr;
	}
	return NULL;
}

hot static inline void
dispatch_mgr_snapshot(DispatchMgr* self, Access* access)
{
	// create ptr for each partition from access
	for (auto i = 0; i < access->list_count; i++)
	{
		auto ac = access_at(access, i);
		if (ac->type == ACCESS_CALL)
			continue;
		auto table = table_of(ac->rel);
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			auto ptr = ptr_create(&self->cache_ptr, part, self->dtr, &self->complete);
			list_append(&self->ptrs, &ptr->link);
			self->ptrs_count++;
		}
	}
}

hot static inline void
dispatch_mgr_send(DispatchMgr* self, Dispatch* dispatch)
{
	// handle snapshot by starting execution of all prepared ptrs
	// on first send
	if (dispatch_mgr_is_first(self) && self->ptrs_count > 0)
	{
		list_foreach(&self->ptrs)
		{
			auto ptr = list_at(Ptr, link);
			pod_send_backend(ptr->part->pod, &ptr->msg);
		}
	}

	// add dispatch
	dispatch->order = self->list_count;

	buf_write(&self->list, &dispatch, sizeof(Dispatch**));
	self->list_count++;

	// send dispatch requests
	list_foreach(&dispatch->list)
	{
		auto req = list_at(Req, link);

		// find or create pod transaction
		auto ptr = dispatch_mgr_find(self, req->part);
		if (! ptr)
		{
			ptr = ptr_create(&self->cache_ptr, req->part, self->dtr, &self->complete);
			list_append(&self->ptrs, &ptr->link);
			self->ptrs_count++;
		}
		ptr->closed = dispatch->close;
		pod_send_backend(ptr->part->pod, &req->msg);
	}

	// send early close requests
	if (dispatch->close)
		dispatch_mgr_close(self);
}

hot static inline void
dispatch_mgr_complete(DispatchMgr* self, Buf** error, bool* write, uint64_t* tsn)
{
	*error = NULL;
	*write = false;
	*tsn   = 0;

	// make sure all transactions complete execution
	auto active = dispatch_mgr_close(self);
	if (! active)
		return;

	// wait for group completion
	complete_wait(&self->complete);

	// sync metrics
	list_foreach(&self->ptrs)
	{
		auto ptr = list_at(Ptr, link);
		if (ptr->error && !*error)
			*error = ptr->error;
		auto tr = ptr->tr;
		if (! tr)
			continue;

		// is writing transaction
		if (! tr_read_only(tr))
			*write = true;

		// sync max tsn across commited transactions
		if (tr->tsn_max > *tsn)
			*tsn = tr->tsn_max;
	}
}
