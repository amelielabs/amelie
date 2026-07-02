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

typedef struct ReqCache ReqCache;

struct ReqCache
{
	List list;
};

static inline void
req_cache_init(ReqCache* self)
{
	list_init(&self->list);
}

static inline void
req_cache_free(ReqCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Req, link);
		req_free_memory(req);
	}
	list_init(&self->list);
}

static inline Req*
req_cache_pop(ReqCache* self)
{
	if (list_empty(&self->list))
		return NULL;
	auto first = list_pop(&self->list);
	return container_of(first, Req, link);
}

static inline void
req_cache_push(ReqCache* self, Req* req)
{
	req_reset(req);
	list_append(&self->list, &req->link);
}

hot static inline Req*
req_create(ReqCache* self)
{
	auto req = req_cache_pop(self);
	if (! req)
		req = req_allocate();
	else
		list_init(&req->link);
	return req;
}
