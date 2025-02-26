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

typedef struct PageMgr PageMgr;

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

static inline void*
page_mgr_at(PageMgr* self, int pos)
{
	return ((void**)self->list.start)[pos];
}

static inline void
page_mgr_free(PageMgr* self)
{
	for (int i = 0; i < self->list_count; i++)
		vfs_munmap(page_mgr_at(self, i), self->page_size);
	self->list_count = 0;
	buf_free(&self->list);
}

static inline uint8_t*
page_mgr_allocate(PageMgr* self)
{
	buf_reserve(&self->list, sizeof(void*));
	uint8_t* page = vfs_mmap(-1, self->page_size);
	if (unlikely(page == NULL))
		error_system();
	buf_append(&self->list, &page, sizeof(void*));
	self->list_count++;;
	return page;
}

hot static inline void*
page_mgr_pointer_of(PageMgr* self, uint64_t addr)
{
	int pos = addr / self->page_size;
	int offset = addr % self->page_size;
	return (uint8_t*)page_mgr_at(self, pos) + offset;
}

hot static inline uint64_t
page_mgr_address_of(PageMgr* self, void* pointer)
{
	uintptr_t ptr = (uintptr_t)pointer;
	for (int i = 0; i < self->list_count; i++)
	{
		uintptr_t start = (uintptr_t)page_mgr_at(self, i);
		if (start <= ptr && ptr < start + self->page_size)
			return i * self->page_size + (ptr - start);
	}
	abort();
	return 0;
}
