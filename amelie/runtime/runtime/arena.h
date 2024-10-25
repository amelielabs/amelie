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
	int        used;
	ArenaPage* next;
	char       data[];
};

struct Arena
{
	int        page_size;
	ArenaPage* list;
	ArenaPage* list_tail;
	int        list_count;
	int        offset;
};

static inline void
arena_init(Arena* self, int page_size)
{
	self->page_size  = page_size;
	self->list       = NULL;
	self->list_tail  = NULL;
	self->list_count = 0;
	self->offset     = 0;
}

static inline void
arena_reset(Arena* self)
{
	auto page = self->list;
	while (page)
	{
		auto next = page->next;
		am_free(page);
		page = next;
	}
	self->list       = NULL;
	self->list_count = 0;
	self->offset     = 0;
}

static inline ArenaPage*
arena_add_page(Arena* self)
{
	ArenaPage* page;
	page = am_malloc(sizeof(ArenaPage) + self->page_size);
	page->used = 0;
	page->next = NULL;
	return page;
}

hot static inline void*
arena_allocate(Arena* self, int size)
{
	if (unlikely(size > self->page_size))
		return NULL;
	ArenaPage* page;
	if (unlikely(self->list == NULL || (self->page_size - self->list_tail->used) < size))
	{
		page = arena_add_page(self);
		if (self->list == NULL)
			self->list = page;
		else
			self->list_tail->next = page;
		self->list_tail = page;
		self->list_count++;
	} else {
		page = self->list_tail;
	}
	void* ptr = page->data + page->used;
	page->used += size;
	self->offset += size;
	return ptr;
}

hot static inline void
arena_truncate(Arena* self, int offset)
{
	assert(offset >= 0 && offset <= self->offset);
	if (self->list == NULL)
		return;

	int page_order = offset / self->page_size;
	int page_pos   = offset % self->page_size;

	ArenaPage* page = self->list;
	int order = 0;
	for (; order < page_order; order++)
		page = page->next;

	ArenaPage* page_next = page->next;
	page->used = page_pos;
	page->next = NULL;

	self->list = page;
	self->list_tail = page;

	while (page_next)
	{
		ArenaPage* next = page_next->next;
		am_free(page_next);
		page_next = next;
		self->list_count--;
	}

	self->offset = offset;
}
