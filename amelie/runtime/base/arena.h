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

typedef struct ArenaPage ArenaPage;
typedef struct Arena     Arena;

struct ArenaPage
{
	size_t     used;
	ArenaPage* prev;
	char       data[];
};

struct Arena
{
	ArenaPage* list;
	int        list_count;
	ArenaPage* list_cache;
	int        list_cache_count;
	size_t     size_page;
};

static inline void
arena_init(Arena* self, size_t size_page)
{
	self->list             = NULL;
	self->list_count       = 0;
	self->list_cache       = NULL;
	self->list_cache_count = 0;
	self->size_page        = size_page;
}

static inline void
arena_reset(Arena* self)
{
	auto page = self->list;
	while (page)
	{
		auto prev = page->prev;
		page->prev = self->list_cache;
		self->list_cache = page;
		self->list_cache_count++;
		page = prev;
	}
	self->list       = NULL;
	self->list_count = 0;
}

static inline void
arena_free(Arena* self)
{
	arena_reset(self);
	auto page = self->list_cache;
	while (page)
	{
		auto prev = page->prev;
		am_free(page);
		page = prev;
	}
	self->list_cache       = NULL;
	self->list_cache_count = 0;
}

hot static inline void*
arena_allocate(Arena* self, size_t size)
{
	assert(size <= self->size_page);
	auto page = self->list;
	if (unlikely(! (page && (self->size_page - page->used) >= size)))
	{
		if (likely(self->list_cache))
		{
			page = self->list_cache;
			self->list_cache = page->prev;
			self->list_cache_count--;
		} else
		{
			page = am_malloc(sizeof(ArenaPage) + self->size_page);
		}
		page->used = 0;
		page->prev = self->list;
		self->list = page;
		self->list_count++;
	}
	auto ptr = page->data + page->used;
	page->used += size;
	return ptr;
}
