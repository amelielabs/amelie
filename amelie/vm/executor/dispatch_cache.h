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

typedef struct DispatchCache DispatchCache;

struct DispatchCache
{
	List list;
};

static inline void
dispatch_cache_init(DispatchCache* self)
{
	list_init(&self->list);
}

static inline void
dispatch_cache_free(DispatchCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto dispatch = list_at(Dispatch, link);
		dispatch_free(dispatch);
	}
	list_init(&self->list);
}

static inline Dispatch*
dispatch_cache_pop(DispatchCache* self)
{
	if (list_empty(&self->list))
		return NULL;
	auto first = list_pop(&self->list);
	return container_of(first, Dispatch, link);
}

static inline void
dispatch_cache_push(DispatchCache* self, Dispatch* dispatch, ReqCache* req_cache) 
{
	dispatch_reset(dispatch, req_cache);
	list_append(&self->list, &dispatch->link);
}

hot static inline Dispatch*
dispatch_create(DispatchCache* self)
{
	auto dispatch = dispatch_cache_pop(self);
	if (! dispatch)
		dispatch = dispatch_allocate();
	else
		list_init(&dispatch->link);
	return dispatch;
}
