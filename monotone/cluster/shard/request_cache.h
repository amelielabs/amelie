#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestCache RequestCache;

struct RequestCache
{
	Spinlock lock;
	int      list_count;
	List     list;
};

static inline void
request_cache_init(RequestCache* self)
{
	self->list_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
request_cache_free(RequestCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto request = list_at(Request, link);
		request_free(request);
	}
	spinlock_free(&self->lock);
}

static inline Request*
request_cache_pop(RequestCache* self)
{
	spinlock_lock(&self->lock);
	Request* request = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		request = container_of(first, Request, link);
		self->list_count--;
	}
	spinlock_unlock(&self->lock);
	return request;
}

static inline void
request_cache_push(RequestCache* self, Request* request)
{
	request_reset(request);
	spinlock_lock(&self->lock);
	list_append(&self->list, &request->link);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

static inline Request*
request_create(RequestCache* self)
{
	auto request = request_cache_pop(self);
	if (request) {
		list_init(&request->link);
	} else
	{
		request = mn_malloc(sizeof(Request));
		request_init(request, self);
		guard(guard, request_free, request);
		channel_attach(&request->src);
		unguard(&guard);
	}
	return request;
}
