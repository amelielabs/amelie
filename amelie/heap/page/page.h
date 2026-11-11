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

enum
{
	PAGE_UNDEF,
	PAGE_HEAP_META,
	PAGE_HEAP,
	PAGE_FLAT_META,
	PAGE_FLAT
};

struct Page
{
	// 64 bytes (aligned by cache line)
	uint32_t crc;
	uint32_t crc_data;
	uint32_t id;
	Uuid     id_rel;
	uint8_t  type;
	uint8_t  compression;
	uint8_t  dirty;
	uint8_t  free;

	// usage
	uint32_t size;
	uint32_t size_compressed;
	uint32_t position;
	uint32_t position_last;
	uint8_t  padding[16];
	uint8_t  data[];
} packed;

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

void    page_save(Page*, File*);
int64_t page_load(Page*, File*, int64_t);
