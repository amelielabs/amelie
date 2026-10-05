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
	PAGE_META,
	PAGE_HEAP,
	PAGE_FLAT
};

struct Page
{
	// 59 bytes (aligned by cache line)
	uint32_t crc;
	uint32_t crc_data;
	uint32_t version;
	Id       id;
	uint8_t  type;
	uint8_t  compression;
	uint8_t  changed;
	uint8_t  reserved[5];
	// usage
	uint32_t size;
	uint32_t size_compressed;
	uint32_t position;
	uint32_t position_last;
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

Page*  page_allocate(uint32_t);
void   page_free(Page*);
Page*  page_load(Id*, uint64_t);
size_t page_save(Page*, uint64_t);
