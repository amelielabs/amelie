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

typedef struct BufCache BufCache;

struct BufCache
{
	Buf**   stack;
	int     stack_count;
	int     stack_size;
	BufMgr* buf_mgr;
	int     limit;
};

static inline void
buf_cache_init(BufCache* self)
{
	self->stack       = NULL;
	self->stack_count = 0;
	self->stack_size  = 0;
	self->buf_mgr     = NULL;
	self->limit       = 0;
}

static inline int
buf_cache_allocate_nothrow(BufCache* self, BufMgr* buf_mgr, int capacity, int limit)
{
	assert(! self->stack);
	self->buf_mgr    = buf_mgr;
	self->limit      = limit;
	self->stack_size = capacity;
	self->stack      =
		am_malloc_aligned_nothrow(capacity * sizeof(Buf*), cache_line);
	if (! self->stack)
		return -1;
	return 0;
}

static inline void
buf_cache_free(BufCache* self)
{
	if (self->stack)
	{
		if (self->stack_count > 0)
			buf_mgr_push(self->buf_mgr, self->stack, self->stack_count);
		am_free(self->stack);
	}
	buf_cache_init(self);
}

hot static inline Buf*
buf_cache_pop(BufCache* self)
{
	// batch move buffers from the global cache to the local cache
	if (unlikely(! self->stack_count))
		self->stack_count += buf_mgr_pop(self->buf_mgr, self->stack, 64);

	// use local cache as priority
	if (likely(self->stack_count > 0))
		return self->stack[--self->stack_count];

	auto buf = (Buf*)am_malloc(sizeof(Buf));
	buf_init(buf);
	buf->allocated = true;
	return buf;
}

hot static inline void
buf_cache_push(BufCache* self, Buf* buf)
{
	if (unlikely(buf_capacity(buf)) >= self->limit)
	{
		buf_free_memory(buf);
		return;
	}
	buf_reset(buf);

	// batch move buffers from the local cache to the global cache
	if (unlikely(self->stack_count == self->stack_size))
	{
		buf_mgr_push(self->buf_mgr, self->stack + (self->stack_count - 64), 64);
		self->stack_count -= 64;
	}

	self->stack[self->stack_count] = buf;
	self->stack_count++;
}
