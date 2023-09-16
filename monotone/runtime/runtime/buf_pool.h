#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct BufPool BufPool;

struct BufPool
{
	List list;
	int  list_count;
};

static inline void
buf_pool_init(BufPool* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline int
buf_pool_size(BufPool *self)
{
	return self->list_count;
}

always_inline static inline void
buf_pool_attach(BufPool* self, Buf* buf)
{
	buf->pool = self;
	list_append(&self->list, &buf->link);
	self->list_count++;
}

always_inline static inline void
buf_pool_detach(Buf* buf)
{
	if (! buf->pool)
		return;
	list_unlink(&buf->link);
	BufPool* self = buf->pool;
	buf->pool = NULL;
	self->list_count--;
}

static inline void
buf_pool_add(BufPool* self, Buf* buf)
{
	if (buf->pool == self)
		return;
	buf_pool_detach(buf);
	buf_pool_attach(self, buf);
}

static inline Buf*
buf_pool_pop(BufPool* self, BufPool* to)
{
	Buf* buf = NULL;
	if (self->list_count > 0)
	{
		auto next = list_pop(&self->list);
		self->list_count--;
		buf = container_of(next, Buf, link);
		buf->pool = NULL;
	}
	if (buf && to)
		buf_pool_add(to, buf);
	return buf;
}

hot static inline void
buf_pool_reset(BufPool* self, BufCache* cache)
{
	for (;;) {
		auto buf = buf_pool_pop(self, NULL);
		if (buf == NULL)
			break;
		buf_cache_push(cache, buf);
	}
	buf_pool_init(self);
}
