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

typedef struct ServicePage ServicePage;

struct ServicePage
{
	uint32_t     size;
	uint32_t     position;
	Action*      last;
	uint32_t     refs;
	ServicePage* next;
	uint8_t      data[];
};

static inline ServicePage*
service_page_allocate(uint32_t size)
{
	auto prot = PROT_READ|PROT_WRITE;
	uint8_t* pointer = mmap(NULL, size, prot, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (unlikely(pointer == MAP_FAILED))
		return NULL;

	if (size == 2 * 1024 * 1024)
		madvise(pointer, size, MADV_HUGEPAGE);

	auto page = (ServicePage*)(pointer);
	page->size     = size;
	page->position = sizeof(ServicePage);
	page->refs     = 0;
	page->last     = NULL;
	page->next     = NULL;
	return page;
}

static inline void
service_page_free(ServicePage* self)
{
	vfs_munmap(self, self->size);
}

always_inline static inline void
service_page_ref(ServicePage* self)
{
	self->refs++;
}

always_inline static inline void
service_page_unref(ServicePage* self)
{
	self->refs--;
}
