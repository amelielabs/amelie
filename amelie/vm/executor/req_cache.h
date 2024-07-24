#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ReqCache ReqCache;

struct ReqCache
{
	ReqList list;
};

static inline void
req_cache_init(ReqCache* self)
{
	req_list_init(&self->list);
}

static inline void
req_cache_free(ReqCache* self)
{
	req_list_free(&self->list);
}

static inline Req*
req_cache_pop(ReqCache* self)
{
	return req_list_get(&self->list);
}

static inline void
req_cache_push(ReqCache* self, Req* req)
{
	req_reset(req);
	req_list_add(&self->list, req);
}

static inline void
req_cache_push_list(ReqCache* self, ReqList* list)
{
	list_foreach_safe(&list->list)
	{
		auto req = list_at(Req, link);
		req_cache_push(self, req);
	}
	req_list_init(list);
}

static inline Req*
req_create(ReqCache* self)
{
	auto req = req_cache_pop(self);
	if (unlikely(! req))
		req = req_allocate();
	return req;
}
