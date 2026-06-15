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

enum
{
	STORAGE_HEAP,
	STORAGE_CDC
};

struct Storage
{
	Page*    current;
	Buf      list;
	int      list_count;
	uint32_t id_first;
	uint32_t id_seq;
	int      size_page;
	int      type;
};

always_inline static inline Page*
storage_at(Storage* self, int id)
{
	return ((Page**)self->list.start)[id - self->id_first];
}

always_inline static inline void*
storage_pointer_of(Storage* self, uint32_t page, int offset)
{
	return page_at(storage_at(self, page), offset);
}

static inline size_t
storage_size(Storage* self)
{
	return self->list_count * self->size_page;
}

static inline bool
storage_empty(Storage* self)
{
	return !self->list_count;
}

static inline void
storage_init(Storage* self, int type, int size_page)
{
	self->current    = NULL;
	self->list_count = 0;
	self->id_first   = 0;
	self->id_seq     = 0;
	self->size_page  = size_page;
	self->type       = type;
	buf_init(&self->list);
}

static inline void
storage_free(Storage* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto page = storage_at(self, i);
		page_free(page);
	}
	buf_free(&self->list);
}

static inline void
storage_import(Storage* self, Page* page)
{
	buf_write(&self->list, &page, sizeof(Page*));
	self->list_count++;

	if (self->list_count == 1)
		self->id_first = page->id;

	if (page->id > self->id_seq)
		self->id_seq = page->id;
}

static inline Page*
storage_add(Storage* self)
{
	// mmap page
	auto page = page_allocate(self->size_page);
	page->id = self->id_seq++;
	self->current = page;

	buf_write(&self->list, &page, sizeof(Page*));
	self->list_count++;
	return page;
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
		self->id_first = ((Page**)self->list.start)[0]->id;
	} else
	{
		self->id_first = 0;
		self->id_seq   = 0;
	}
	return page;
}
