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

typedef struct Palloc Palloc;

struct Palloc
{
	Buf*      list;
	int       list_count;
	size_t    size_page;
	BufCache* buf_cache;
};

always_inline static inline Buf*
palloc_at(Palloc* self, int order)
{
	return ((Buf**)self->list->start)[order];
}

always_inline static inline Buf*
palloc_last(Palloc* self)
{
	return palloc_at(self, self->list_count - 1);
}

static inline void
palloc_init(Palloc* self, BufCache* buf_cache, size_t size_page)
{
	self->list       = NULL;
	self->list_count = 0;
	self->size_page  = size_page;
	self->buf_cache  = buf_cache;
}

static inline void
palloc_reset(Palloc* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto buf = palloc_at(self, i);
		buf_cache_push(self->buf_cache, buf);
	}
	self->list_count = 0;
	if (self->list)
	{
		buf_cache_push(self->buf_cache, self->list);
		self->list = NULL;
	}
}

static inline void
palloc_free(Palloc* self)
{
	palloc_reset(self);
}

hot static inline void*
palloc_allocate(Palloc* self, size_t size)
{
	assert(size <= self->size_page);
	if (!self->list || (self->size_page - buf_size(palloc_last(self))) < size)
	{
		if (unlikely(! self->list))
			self->list = buf_cache_pop(self->buf_cache);

		auto buf = buf_cache_pop(self->buf_cache);
		buf_write(self->list, &buf, sizeof(buf));
		self->list_count++;

		buf_reserve(buf, self->size_page);
	}

	auto current = palloc_last(self);
	auto ptr = current->position;
	buf_advance(current, size);
	return ptr;
}
