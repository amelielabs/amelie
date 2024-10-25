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
	int      list_count;
	List     list;
	int      limit_buf;
};

static inline void
buf_cache_init(BufCache* self, int limit_buf)
{
	self->list_count = 0;
	self->limit_buf  = limit_buf;
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
buf_cache_free(BufCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto buf = list_at(Buf, link);
		buf_free_memory(buf);
	}
	list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline Buf*
buf_cache_pop(BufCache* self)
{
	spinlock_lock(&self->lock);
	Buf* buf = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		buf = container_of(first, Buf, link);
		self->list_count--;
	}
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
	list_init(&buf->link);
	spinlock_lock(&self->lock);
	list_append(&self->list, &buf->link);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

static inline Buf*
buf_cache_create(BufCache* self, int size)
{
	auto buf = buf_cache_pop(self);
	if (buf) {
		list_init(&buf->link);
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
