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

typedef struct ServiceReqCache ServiceReqCache;

struct ServiceReqCache
{
	Spinlock lock;
	List     list;
};

static inline void
service_req_cache_init(ServiceReqCache* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
service_req_cache_free(ServiceReqCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(ServiceReq, link);
		service_req_free(req);
	}
	list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline ServiceReq*
service_req_cache_pop(ServiceReqCache* self)
{
	spinlock_lock(&self->lock);
	ServiceReq* req = NULL;
	if (! list_empty(&self->list))
	{
		auto first = list_pop(&self->list);
		req = container_of(first, ServiceReq, link);
	}
	spinlock_unlock(&self->lock);
	return req;
}

static inline void
service_req_cache_push(ServiceReqCache* self, ServiceReq* req)
{
	spinlock_lock(&self->lock);
	list_append(&self->list, &req->link);
	spinlock_unlock(&self->lock);
}

static inline ServiceReq*
service_req_create(ServiceReqCache* self)
{
	auto req = service_req_cache_pop(self);
	if (unlikely(! req))
		req = service_req_allocate();
	else
		service_req_init(req);
	return req;
}
