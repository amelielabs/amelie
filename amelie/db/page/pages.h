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

typedef struct Pages Pages;

struct Pages
{
	Page*      current;
	Page*      meta;
	Buf        list;
	int        list_count;
	Retention* retention;
	int        type;
	int        type_meta;
	uint32_t   id_first;
	uint32_t   id_next;
	int        size;
};

void  pages_init(Pages*, Retention*, int, int);
void  pages_free(Pages*);
Page* pages_add_meta(Pages*, int);
Page* pages_add(Pages*);
void  pages_open(Pages*, uint64_t, int, int64_t*);
bool  pages_retention(Pages*, uint32_t*);

always_inline static inline Page*
pages_at(Pages* self, int order)
{
	return ((Page**)self->list.start)[order];
}

always_inline static inline Page*
pages_get(Pages* self, uint32_t id)
{
	return pages_at(self, id - self->id_first);
}

always_inline static inline void*
pages_pointer_of(Pages* self, uint32_t page, int offset)
{
	return page_at(pages_get(self, page), offset);
}

static inline bool
pages_empty(Pages* self)
{
	return !self->list_count;
}

static inline size_t
pages_size(Pages* self)
{
	return self->list_count * self->size;
}

hot static inline bool
pages_ensure(Pages* self, uint32_t size)
{
	// ensure size can fit the page
	uint32_t max = self->size - sizeof(Page);
	if (unlikely(size > max))
		error("storage: max page capacity {u32} exceeded", max);

	// add new page
	auto page = self->current;
	if (unlikely(!page || (page->size - page->position) < size))
	{
		pages_add(self);
		return true;
	}
	return false;
}

always_inline static inline int64_t
pages_delta(Pages* self, Page* page)
{
	if (likely(self->current == page))
		return 0;
	return self->current->size;
}
