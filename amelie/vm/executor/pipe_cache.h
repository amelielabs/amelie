#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct PipeCache PipeCache;

struct PipeCache
{
	int  list_count;
	List list;
};

static inline void
pipe_cache_init(PipeCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
pipe_cache_free(PipeCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto pipe = list_at(Pipe, link);
		pipe_free(pipe);
	}
}

static inline Pipe*
pipe_cache_pop(PipeCache* self)
{
	if (self->list_count == 0)
		return NULL;
	auto first = list_pop(&self->list);
	auto pipe = container_of(first, Pipe, link);
	self->list_count--;
	return pipe;
}

static inline void
pipe_cache_push(PipeCache* self, Pipe* pipe)
{
	pipe_reset(pipe);
	list_append(&self->list, &pipe->link);
	self->list_count++;
}

static inline Pipe*
pipe_create(PipeCache* self, Route* route)
{
	auto pipe = pipe_cache_pop(self);
	if (unlikely(! pipe))
	{
		pipe = pipe_allocate();
		guard(pipe_free, pipe);
		channel_attach(&pipe->src);
		unguard();
	}
	pipe->route = route;
	return pipe;
}
