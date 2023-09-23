#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestCache RequestCache;

struct RequestCache
{
	List list;
	int  list_count;
};

static inline void
request_cache_init(RequestCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
request_cache_free(RequestCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Request, link);
		request_free(req);
	}
}

static inline Request*
request_cache_pop(RequestCache* self)
{
	Request* req = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		req = container_of(first, Request, link);
		self->list_count--;
	}
	return req;
}

static inline void
request_cache_push(RequestCache* self, Request* req)
{
	event_set_parent(&req->src.on_write.event, NULL);
	list_init(&req->link);
	list_append(&self->list, &req->link);
	self->list_count++;
}

static inline Request*
request_create(RequestCache* self, Channel* dst)
{
	auto req = request_cache_pop(self);
	if (req) {
		request_reset(req);
	} else
	{
		req = request_allocate();
		guard(req_guard, request_free, req);
		channel_attach(&req->src);
		unguard(&req_guard);
	}
	req->dst = dst;
	return req;
}
