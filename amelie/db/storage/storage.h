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

typedef struct Storage Storage;

struct Storage
{
	Page*    current;
	Page*    meta;
	Buf      list;
	int      list_count;
	int      type;
	PageId   id;
	uint32_t id_first;
	uint32_t id_next;
	int      size;
};

always_inline static inline Page*
storage_at(Storage* self, int order)
{
	return ((Page**)self->list.start)[order];
}

always_inline static inline Page*
storage_get(Storage* self, uint32_t id)
{
	return storage_at(self, id - self->id_first);
}

always_inline static inline void*
storage_pointer_of(Storage* self, uint32_t page, int offset)
{
	return page_at(storage_get(self, page), offset);
}

static inline void
storage_init(Storage* self, int type)
{
	self->current    = NULL;
	self->meta       = NULL;
	self->list_count = 0;
	self->type       = type;
	self->id_first   = 0;
	self->id_next    = 0;
	self->size       = 64 * 1024 * 1024;
	buf_init(&self->list);
	page_id_init(&self->id);
}

static inline void
storage_free(Storage* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto page = storage_at(self, i);
		page_free(page);
	}
	if (self->meta)
		page_free(self->meta);
	buf_free(&self->list);
}

static inline size_t
storage_size(Storage* self)
{
	return self->list_count * self->size;
}

static inline bool
storage_empty(Storage* self)
{
	return !self->list_count;
}

static inline Page*
storage_add_meta(Storage* self, int size)
{
	assert(! self->meta);
	// create and set meta page
	auto page = page_allocate(size);
	page->type       = PAGE_META;
	page->id         = self->id;
	page->id.id_page = UINT32_MAX;
	self->meta       = page;
	return page;
}

static inline Page*
storage_add(Storage* self)
{
	// create new page
	auto page = page_allocate(self->size);
	page->type       = self->type;
	page->id         = self->id;
	page->id.id_page = self->id_next++;
	self->current    = page;

	buf_write(&self->list, &page, sizeof(Page*));
	self->list_count++;
	return page;
}

static inline bool
storage_ensure(Storage* self, uint32_t size)
{
	// ensure size can fit the page
	uint32_t max = self->size - sizeof(Page);
	if (unlikely(size > max))
		error("storage: max page capacity {u32} exceeded", max);

	// add new page
	auto page = self->current;
	if (unlikely(!page || (page->size - page->position) < size))
	{
		storage_add(self);
		return true;
	}
	return false;
}

static inline Page*
storage_pop(Storage* self)
{
	if (unlikely(! self->list_count))
		return NULL;

	auto page = storage_at(self, 0);
	self->list_count--;

	auto to_move = self->list_count * sizeof(Page*);
	memmove(self->list.start, self->list.start + sizeof(Page*), to_move);
	buf_truncate(&self->list, sizeof(Page*));

	if (self->list_count > 0)
	{
		self->id_first = ((Page**)self->list.start)[0]->id.id_page;
	} else
	{
		self->id_first = 0;
		self->id_next  = 0;
	}
	return page;
}

always_inline static inline int64_t
storage_delta(Storage* self, Page* page)
{
	if (likely(self->current == page))
		return 0;
	return self->current->size;
}

#if 0
static inline void
storage_import(Storage* self, Page* page)
{
	buf_write(&self->list, &page, sizeof(Page*));
	self->list_count++;

	if (self->list_count == 1)
	{
		self->id_first = page->id;
		self->id_seq   = page->id;
	}

	if (page->id > self->id_seq)
		self->id_seq = page->id;

	self->current = page;

	// todo: (after each page loaded)
		// advance sequence id for a next page
		if (self->list_count > 0)
			self->id_next++;
}
#endif
