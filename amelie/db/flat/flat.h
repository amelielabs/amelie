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

typedef struct FlatRow FlatRow;
typedef struct Flat    Flat;

struct FlatRow
{
	// 64bit
	uint64_t row_page: 19;
	uint64_t row_offset: 19;
	uint64_t used: 1;
	uint64_t padding: 25;
} packed;

struct Flat
{
	uint32_t per_page;
	Column*  column;
	Storage  storage;
	List     link;
};

always_inline static inline FlatRow*
flat_row(Flat* self, uint32_t id)
{
	auto page_id  = id / self->per_page;
	auto page_row = id % self->per_page;
	auto page = storage_at(&self->storage, page_id);
	return &((FlatRow*)page->data)[page_row];
}

always_inline static inline void*
flat_at(Flat* self, uint32_t id)
{
	auto page_id  = id / self->per_page;
	auto page_row = id % self->per_page;
	auto page = storage_at(&self->storage, page_id);
	return page->data + (sizeof(FlatRow) * self->per_page) +
	       page_row * self->column->size_flat;
}

Flat*    flat_allocate(Column*);
void     flat_free(Flat*);
uint32_t flat_add(Flat*, int, int);
void     flat_remove(Flat*, uint32_t);
