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

typedef struct PageHeader PageHeader;
typedef struct Page       Page;
typedef struct PageMgr    PageMgr;

struct PageHeader
{
	uint32_t size;
	uint32_t last;
} packed;

struct Page
{
	uint8_t* pointer;
};

struct PageMgr
{
	int page_size;
	int list_count;
	Buf list;
};

static inline void
page_mgr_init(PageMgr* self)
{
	self->page_size  = 2 * 1024 * 1024;
	self->list_count = 0;
	buf_init(&self->list);
}

always_inline static inline Page*
page_mgr_at(PageMgr* self, int pos)
{
	return &((Page*)self->list.start)[pos];
}

static inline void
page_mgr_free(PageMgr* self)
{
	for (int i = 0; i < self->list_count; i++)
	{
		auto page = page_mgr_at(self, i);
		vfs_munmap(page->pointer, self->page_size);
	}
	self->list_count = 0;
	buf_free(&self->list);
}

static inline Page*
page_mgr_allocate(PageMgr* self)
{
	uint8_t* pointer = vfs_mmap(-1, self->page_size);
	if (unlikely(pointer == NULL))
		error_system();
	Page* page = buf_claim(&self->list, sizeof(Page));
	page->pointer = pointer;
	self->list_count++;
	return page;
}

always_inline static inline void*
page_mgr_pointer_of(PageMgr* self, int page, int offset)
{
	return page_mgr_at(self, page)->pointer + offset;
}
