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
	StorageHeader header;
	PageHeader*   last;
	int           list_count;
	Buf           list;
};

always_inline static inline Page*
storage_at(Storage* self, int pos)
{
	return &((Page*)self->list.start)[pos];
}

always_inline static inline void*
storage_pointer_of(Storage* self, int page, int offset)
{
	return storage_at(self, page)->pointer + offset;
}

static inline void
storage_init(Storage* self, int type, int size_page)
{
	memset(&self->header, 0, sizeof(self->header));
	self->header.version   = STORAGE_VERSION;
	self->header.magic     = STORAGE_MAGIC;
	self->header.type      = type;
	self->header.size_page = size_page;
	self->last       = NULL;
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
storage_free(Storage* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto page = storage_at(self, i);
		vfs_munmap(page->pointer, self->header.size_page);
	}
	self->list_count = 0;
	buf_free(&self->list);
}

static inline size_t
storage_size(Storage* self)
{
	return self->list_count * self->header.size_page;
}

static inline Page*
storage_add(Storage* self)
{
	// mmap
	uint8_t* pointer = vfs_mmap(-1, self->header.size_page);
	if (unlikely(pointer == NULL))
		error_system();

	// add page
	Page* page = buf_emplace(&self->list, sizeof(Page));
	page->pointer = pointer;
	self->list_count++;
	self->last = (PageHeader*)page->pointer;

	// prepare header
	auto header = self->last;
	header->crc             = 0;
	header->size            = sizeof(PageHeader);
	header->size_compressed = 0;
	header->order           = self->list_count - 1;
	header->last            = 0;
	header->padding         = 0;
	return page;
}
