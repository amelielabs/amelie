#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct PipeSet PipeSet;

struct PipeSet
{
	Pipe** set;
	int    set_size;
	int    set_allocated;
	Buf    buf;
};

static inline void
pipe_set_init(PipeSet* self)
{
	self->set           = NULL;
	self->set_size      = 0;
	self->set_allocated = 0;
	buf_init(&self->buf);
}

static inline void
pipe_set_reset(PipeSet* self, PipeCache* cache)
{
	if (cache)
	{
		for (int i = 0; i < self->set_size; i++)
		{
			auto pipe = self->set[i];
			if (! pipe)
				continue;
			pipe_cache_push(cache, pipe);
		}
	}
	self->set           = NULL;
	self->set_size      = 0;
	self->set_allocated = 0;
	buf_reset(&self->buf);
}

static inline void
pipe_set_free(PipeSet* self)
{
	buf_free(&self->buf);
}

static inline void
pipe_set_create(PipeSet* self, int set_size)
{
	self->set_size      = set_size;
	self->set_allocated = sizeof(Pipe*) * set_size;
	buf_reserve(&self->buf, self->set_allocated);
	memset(self->buf.start, 0, self->set_allocated);
	self->set = (Pipe**)self->buf.start;
}

hot static inline void
pipe_set_resolve(PipeSet* self, PipeSet* set)
{
	for (int i = 0; i < self->set_size; i++)
	{
		if (self->set[i] == NULL)
			continue;
		set->set[i] = self->set[i];
	}
}

always_inline static inline void
pipe_set_set(PipeSet* self, int order, Pipe* pipe)
{
	self->set[order] = pipe;
}

always_inline static inline Pipe*
pipe_set_get(PipeSet* self, int order)
{
	return self->set[order];
}

hot static inline void
pipe_set_begin(PipeSet* self)
{
	// send pipe pointer
	for (int i = 0; i < self->set_size; i++)
	{
		auto pipe = pipe_set_get(self, i);
		if (pipe == NULL)
			continue;
		auto buf = msg_create(RPC_BEGIN);
		buf_write(buf, &pipe, sizeof(void**));
		msg_end(buf);
		channel_write(pipe->route->channel, buf);
	}
}

hot static inline void
pipe_set_commit(PipeSet* self)
{
	// send pipe transaction pointer
	for (int i = 0; i < self->set_size; i++)
	{
		auto pipe = pipe_set_get(self, i);
		if (pipe == NULL)
			continue;
		assert(pipe->tr);
		auto buf = msg_create(RPC_COMMIT);
		buf_write(buf, &pipe->tr, sizeof(void**));
		msg_end(buf);
		channel_write(pipe->route->channel, buf);
	}
}

hot static inline void
pipe_set_abort(PipeSet* self)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto pipe = pipe_set_get(self, i);
		if (pipe == NULL)
			continue;
		auto buf = msg_create(RPC_ABORT);
		msg_end(buf);
		channel_write(pipe->route->channel, buf);
	}
}
