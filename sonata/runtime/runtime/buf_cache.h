#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct BufCache BufCache;

struct BufCache
{
	Spinlock lock;
	int      list_count;
	List     list;
};

static inline void
buf_cache_init(BufCache* self)
{
	self->list_count = 0;
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
	list_init(&buf->link);
	spinlock_lock(&self->lock);
	list_append(&self->list, &buf->link);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

static inline Buf*
buf_create_nothrow(BufCache* self, int size)
{
	auto buf = buf_cache_pop(self);
	if (buf) {
		list_init(&buf->link);
		buf_reset(buf);
	} else
	{
		buf = so_malloc_nothrow(sizeof(Buf));
		if (unlikely(buf == NULL))
			return NULL;
		buf_init(buf);
		buf->cache = self;
	}
	if (unlikely(buf_reserve_nothrow(buf, size) == -1))
	{
		buf_free_memory(buf);
		return NULL;
	}
	return buf;
}
