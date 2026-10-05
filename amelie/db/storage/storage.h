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
	int      meta_fd;
	Buf      list;
	Buf      list_fd;
	int      list_count;
	int      type;
	Id       id;
	uint32_t id_first;
	uint32_t id_next;
	int      size;
};

always_inline static inline Page*
storage_at(Storage* self, int order)
{
	return ((Page**)self->list.start)[order];
}

always_inline static inline int*
storage_at_fd(Storage* self, int order)
{
	return &((int*)self->list_fd.start)[order];
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

static inline bool
storage_empty(Storage* self)
{
	return !self->list_count;
}

static inline size_t
storage_size(Storage* self)
{
	return self->list_count * self->size;
}

void  storage_init(Storage*f, Id*, int);
void  storage_free(Storage*);
Page* storage_add_meta(Storage*, int);
Page* storage_add(Storage*);
void  storage_open(Storage*, uint64_t, Ids*, Id*);

hot static inline bool
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

always_inline static inline int64_t
storage_delta(Storage* self, Page* page)
{
	if (likely(self->current == page))
		return 0;
	return self->current->size;
}
