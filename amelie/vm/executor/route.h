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

typedef struct Route Route;

struct Route
{
	Mutex    lock;
	CondVar  cond_var;
	int      id;
	bool     shutdown;
	List     list;
	int      list_count;
	TrList   prepared;
	TrCache  cache;
	ReqCache cache_req;
};

static inline void
route_init(Route* self)
{
	self->id         = 0;
	self->list_count = 0;
	self->shutdown   = true;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->list);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	req_cache_init(&self->cache_req);
}

static inline void
route_free(Route* self)
{
	assert(! self->list_count);
	mutex_free(&self->lock);
	cond_var_init(&self->cond_var);
	tr_cache_free(&self->cache);
	req_cache_free(&self->cache_req);
}

static inline void
route_shutdown(Route* self)
{
	mutex_lock(&self->lock);
	self->shutdown = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
route_add(Route* self, Req* req)
{
	mutex_lock(&self->lock);
	self->list_count++;
	list_append(&self->list, &req->link_route);
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline Req*
route_get(Route* self)
{
	mutex_lock(&self->lock);
	Req* next = NULL;
	for (;;)
	{
		if (self->list_count > 0)
		{
			auto ref = list_pop(&self->list);
			self->list_count--;
			next = container_of(ref, Req, link_route);
			break;
		}
		if (self->shutdown)
			break;
		cond_var_wait(&self->cond_var, &self->lock);
	}
	mutex_unlock(&self->lock);
	return next;
}
