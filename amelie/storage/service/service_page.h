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
	uint32_t id;
	uint32_t size;
	uint32_t position;
	uint8_t  data[];
} packed;

always_inline static inline Action*
service_page_at(ServicePage* self, uint32_t offset)
{
	return (Action*)((uint8_t*)self + offset);
}

always_inline static inline uintptr_t
service_page_end(ServicePage* self)
{
	return (uintptr_t)self + self->position;
}

static inline ServicePage*
service_page_allocate(uint32_t size)
{
	// allocate page
	auto prot = PROT_READ|PROT_WRITE;
	uint8_t* pointer = mmap(NULL, size, prot, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (unlikely(pointer == MAP_FAILED))
		error_system();

	auto page = (ServicePage*)(pointer);
	page->id       = 0;
	page->size     = size;
	page->position = sizeof(ServicePage);
	return page;
}

static inline void
service_page_free(ServicePage* self)
{
	vfs_munmap(self, self->size);
}

static inline Action*
service_page_write(ServicePage* self, Action* act)
{
	auto size = action_size(act);
	assert((self->size - self->position) >= size);

	auto position = self->position;
	memcpy(service_page_at(self, position), act, size);
	auto ref = service_page_at(self, position);
	ref->page = self->id;

	self->position += size;
	return ref;
}
