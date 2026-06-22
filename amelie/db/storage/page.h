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

typedef struct Page Page;

struct Page
{
	// 24 bytes (aligned by pointer)
	uint32_t crc;
	uint32_t id;
	uint32_t size;
	uint32_t size_compressed;
	uint32_t position;
	uint32_t used;
	uint8_t  data[];
} packed;

static inline Page*
page_allocate(size_t size)
{
	Page* self = vfs_mmap(-1, size);
	if (unlikely(self == NULL))
		error_system();
	self->crc             = 0;
	self->id              = 0;
	self->size            = size;
	self->size_compressed = 0;
	self->position        = sizeof(Page);
	self->used            = 0;
	return self;
}

static inline void
page_free(Page* self)
{
	vfs_munmap(self, self->size);
}

always_inline static inline uint8_t*
page_at(Page* self, uint32_t offset)
{
	return (uint8_t*)self + offset;
}

always_inline static inline uintptr_t
page_end(Page* self)
{
	return (uintptr_t)self + self->position;
}
