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

typedef struct ReqMgr ReqMgr;

struct ReqMgr
{
	atomic_u64 seq;
	ReqCache   cache[8];
};

static inline void
req_mgr_init(ReqMgr* self)
{
	self->seq = 0;
	for (int i = 0; i < 8; i++)
		req_cache_init(&self->cache[i]);
}

static inline void
req_mgr_free(ReqMgr* self)
{
	self->seq = 0;
	for (int i = 0; i < 8; i++)
		req_cache_free(&self->cache[i]);
}

hot static inline Req*
req_create(ReqMgr* self)
{
	int next = atomic_u64_inc(&self->seq) % 8;
	return req_cache_create(&self->cache[next]);
}

static inline void
req_free(Req* self)
{
	if (likely(self->cache))
	{
		req_cache_push(self->cache, self);
		return;
	}
	req_free_memory(self);
}

static inline void
req_free_list(ReqList* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Req, link);
		req_free(req);
	}
	req_list_init(self);
}
