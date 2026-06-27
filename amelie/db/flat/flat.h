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

typedef struct FlatHeader FlatHeader;
typedef union  FlatRow    FlatRow;
typedef struct Flat       Flat;

struct FlatHeader
{
	uint32_t list_free;
} packed;

union FlatRow
{
	// row reference (64 bit)
	struct
	{
		uint64_t row_page:   19;
		uint64_t row_offset: 19;
		uint64_t padding:    26;
	} packed;

	// free list
	uint32_t row_next;
};

struct Flat
{
	uint32_t   page_rows;
	uint32_t   page_bitmap;
	FlatHeader header;
	Column*    column;
	Storage    storage;
};

always_inline static inline FlatRow*
flat_row(Flat* self, int page_id, int page_row)
{
	// [page_header][bitmap][rows][vectors]
	auto page = storage_at(&self->storage, page_id);
	return &((FlatRow*)(page->data + self->page_bitmap))[page_row];
}

always_inline static inline float*
flat_vector(Flat* self, int page_id, int page_row)
{
	auto page = storage_at(&self->storage, page_id);
	return (float*)(page->data + self->page_bitmap + self->page_rows * sizeof(FlatRow) +
	                page_row * self->column->size_flat);
}

always_inline static inline FlatRow*
flat_row_at(Flat* self, uint32_t id)
{
	auto page_id  = id / self->page_rows;
	auto page_row = id % self->page_rows;
	return flat_row(self, page_id, page_row);
}

always_inline static inline float*
flat_vector_at(Flat* self, uint32_t id)
{
	auto page_id  = id / self->page_rows;
	auto page_row = id % self->page_rows;
	return flat_vector(self, page_id, page_row);
}

always_inline static inline void
flat_set(Flat* self, int page_id, int page_row, bool active)
{
	auto page = storage_at(&self->storage, page_id);
	auto bitmap    = (uint64_t*)(page->data);
	auto bucket    = page_row >> 6;
	auto bit_index = page_row & 63;
	auto mask      = 1ULL << bit_index;
	if (active)
		bitmap[bucket] |= mask;
	else
		bitmap[bucket] &= ~mask;
}

always_inline static inline void
flat_set_at(Flat* self, uint32_t id, bool active)
{
	auto page_id  = id / self->page_rows;
	auto page_row = id % self->page_rows;
	flat_set(self, page_id, page_row, active);
}

Flat*    flat_allocate(Column*);
void     flat_free(Flat*);
size_t   flat_create(Flat*, char*);
size_t   flat_open(Flat*, char*);
uint32_t flat_add(Flat*, int, int);
void     flat_remove(Flat*, uint32_t);
