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
	Buf* head;
	char pad[cache_line - sizeof(Buf*)];
};

static inline void
buf_cache_init(BufCache* self)
{
	self->head = NULL;
}

static inline void
buf_cache_free(BufCache* self)
{
	auto buf = self->head;
	while (buf)
	{
		auto next = buf->cache_next;
		buf_free_memory(buf);
		buf = next;
	}
}

hot static inline Buf*
buf_cache_pop(BufCache* self)
{
	for (;;)
	{
		auto head = __atomic_load_n(&self->head, __ATOMIC_ACQUIRE);
		if (! head)
			break;
		auto next = head->cache_next;
		if (__atomic_compare_exchange_n(&self->head, &head, next, 0,
		                                __ATOMIC_RELEASE,
		                                __ATOMIC_RELAXED))
			return head;
	}
	return NULL;
}

hot static inline void
buf_cache_push(BufCache* self, Buf* buf)
{
	if (unlikely(buf_capacity(buf)) >= 32 * 1024)
	{
		buf_free_memory(buf);
		return;
	}
	for (;;)
	{
		auto head = __atomic_load_n(&self->head, __ATOMIC_RELAXED);
		buf->cache_next = head;
		if (__atomic_compare_exchange_n(&self->head, &head, buf, 1,
		                                __ATOMIC_RELEASE,
		                                __ATOMIC_RELAXED))
			return;
	}
}

hot static inline Buf*
buf_cache_create(BufCache* self, int size)
{
	auto buf = buf_cache_pop(self);
	if (likely(buf)) {
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
