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

struct Dispatch
{
	int      order;
	List     list;
	int      list_count;
	Complete complete;
	bool     returning;
	List     link;
};

static inline Dispatch*
dispatch_allocate(void)
{
	auto self = (Dispatch*)am_malloc(sizeof(Dispatch));
	self->order      = 0;
	self->list_count = 0;
	self->returning  = false;
	list_init(&self->list);
	list_init(&self->link);
	complete_init(&self->complete);
	return self;
}

static inline void
dispatch_free(Dispatch* self)
{
	assert(! self->list_count);
	am_free(self);
}

static inline void
dispatch_reset(Dispatch* self, ReqCache* cache)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Req, link);
		req_cache_push(cache, req);
	}
	list_init(&self->list);
	self->list_count = 0;
	self->returning  = false;
	complete_reset(&self->complete);
}

static inline void
dispatch_set_returning(Dispatch* self)
{
	self->returning = true;
}

static inline void
dispatch_prepare(Dispatch* self)
{
	assert(self->list_count > 0);
	complete_prepare(&self->complete, self->list_count);
}

static inline Req*
dispatch_add(Dispatch* self, ReqCache* cache, int type,
             int       start,
             Code*     code,
             CodeData* code_data,
             Core*     core)
{
	auto req = req_create(cache);
	req->type      = type;
	req->start     = start;
	req->code      = code;
	req->code_data = code_data;
	req->core      = core;
	req->dispatch  = self;
	list_append(&self->list, &req->link);
	self->list_count++;
	return req;
}

static inline void
dispatch_wait(Dispatch* self)
{
	// wait for dispatch completion
	assert(self->returning);
	complete_wait(&self->complete);
}

static inline void
dispatch_complete(Dispatch* self)
{
	if (self->returning)
		complete_signal(&self->complete);
}
