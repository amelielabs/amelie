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
	Spinlock lock;
	Buf*     cache;
	int      limit_buf;
};

static inline void
buf_cache_init(BufCache* self, int limit_buf)
{
	self->cache     = NULL;
	self->limit_buf = limit_buf;
	spinlock_init(&self->lock);
}

static inline void
buf_cache_free(BufCache* self)
{
	auto buf = self->cache;
	while (buf)
	{
		auto next = buf->cache_next;
		buf_free_memory(buf);
		buf = next;
	}
	self->cache = NULL;
	spinlock_free(&self->lock);
}

static inline Buf*
buf_cache_pop(BufCache* self)
{
	spinlock_lock(&self->lock);
	Buf* buf = self->cache;
	if (buf)
		self->cache = buf->cache_next;
	spinlock_unlock(&self->lock);
	return buf;
}

static inline void
buf_cache_push(BufCache* self, Buf* buf)
{
	if (unlikely(buf_capacity(buf)) >= self->limit_buf)
	{
		buf_free_memory(buf);
		return;
	}
	spinlock_lock(&self->lock);
	buf->cache_next = self->cache;
	self->cache = buf;
	spinlock_unlock(&self->lock);
}

static inline Buf*
buf_cache_create(BufCache* self, int size)
{
	auto buf = buf_cache_pop(self);
	if (buf) {
		buf->cache_next = NULL;
		buf_reset(buf);
	} else
	{
		buf = am_malloc(sizeof(Buf));
		buf_init(buf);
		buf->cache = self;
	}
	buf_reserve(buf, size);
	return buf;
}
