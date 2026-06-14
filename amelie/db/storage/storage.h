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

typedef struct Storage       Storage;
typedef struct StorageHeader StorageHeader;

#define STORAGE_MAGIC   0x20849610
#define STORAGE_VERSION 0

enum
{
	STORAGE_HEAP,
	STORAGE_CDC
};

struct StorageHeader
{
	uint32_t crc;
	uint32_t magic;
	uint32_t version;
	uint8_t  type;
	uint8_t  compression;
	uint32_t count;
	uint32_t size_page;
	uint32_t size_meta;
} packed;

struct Storage
{
	Page*         current;
	Buf           list;
	int           list_count;
	StorageHeader header;
};

always_inline static inline Page*
storage_at(Storage* self, int pos)
{
	return ((Page**)self->list.start)[pos];
}

always_inline static inline void*
storage_pointer_of(Storage* self, int page, int offset)
{
	return page_at(storage_at(self, page), offset);
}

static inline size_t
storage_size(Storage* self)
{
	return self->list_count * self->header.size_page;
}

static inline void
storage_init(Storage* self, int type, int size_page)
{
	memset(&self->header, 0, sizeof(self->header));
	self->header.version   = STORAGE_VERSION;
	self->header.magic     = STORAGE_MAGIC;
	self->header.type      = type;
	self->header.size_page = size_page;
	self->current = NULL;

	self->list_count = 0;
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
	self->list_count = 0;
	buf_free(&self->list);
}

static inline Page*
storage_add(Storage* self)
{
	// mmap page
	auto page = page_allocate(self->header.size_page);
	page->id = self->list_count;

	buf_write(&self->list, &page, sizeof(Page*));
	self->list_count++;

	self->current = page;
	return page;
}

static inline Page*
storage_pop(Storage* self)
{
	auto page = storage_at(self, 0);
	assert(self->list_count > 0);
	self->list_count--;

	auto to_move = self->list_count * sizeof(Page*);
	memmove(self->list.start, self->list.start + sizeof(Page*), to_move);
	buf_truncate(&self->list, sizeof(Page*));
	return page;
}
