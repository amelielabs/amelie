
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

#include <amelie_runtime>
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>

Flat*
flat_allocate(Column* column)
{
	auto self = (Flat*)am_malloc(sizeof(Flat));
	self->dim = column->size_flat / sizeof(float);

	//
	// calculate how many vectors can fit the page, we use 64 vectors
	// buckets (64bit bitmap)
	//
	// [page_header][bitmap][vectors_sq8][rows][vectors]
	//
	auto size_page   = (64 * 1024 * 1024);
	auto size_bucket =
		sizeof(uint64_t) +         // 64 vectors bitmap
		(64 * self->dim) +         // 64 vectors in SQ8 encoding (i8)
		(64 * sizeof(FlatRow)) +   // 64 vectors rows refs
		(64 * column->size_flat);  // 64 vectors

	auto buckets = (size_page - sizeof(Page)) / size_bucket;
	self->page_rows   = buckets * 64;
	self->page_bitmap = buckets * sizeof(uint64_t);

	// set page offsets

	// align SQ8 at 64 bytes (for AVX/NEON)
	self->page_offset_i8      = (self->page_bitmap + 64 - 1) & ~(64 - 1);
	self->page_offset_rows    = self->page_offset_i8 + (self->page_rows * self->dim);
	self->page_offset_vectors = self->page_offset_rows + (self->page_rows * sizeof(FlatRow));
	self->column = column;

	auto storage = &self->storage;
	storage_init(storage, PAGE_FLAT);
	storage_add_meta(storage, sizeof(FlatHeader));

	self->header = (FlatHeader*)storage->meta->data;
	self->header->list_free = UINT32_MAX;
	return self;
}

void
flat_free(Flat* self)
{
	storage_free(&self->storage);
	am_free(self);
}

void
flat_open(Flat* self)
{
	// set header
	auto storage = &self->storage;
	assert(!storage->meta && storage->meta->size == sizeof(FlatHeader));
	self->header = (FlatHeader*)storage->meta->data;
}

uint32_t
flat_add(Flat* self, int row_page, int row_offset)
{
	auto id = self->header->list_free;
	if (likely(id != UINT32_MAX))
	{
		auto page_id  = id / self->page_rows;
		auto page_row = id % self->page_rows;
		auto row = flat_row(self, page_id, page_row);

		// mark row as being used and update free list
		self->header->list_free = row->row_next;
		self->storage.meta->changed = true;

		row->row_page   = row_page;
		row->row_offset = row_offset;
		row->padding    = 0;
		flat_set(self, page_id, page_row, true);
		return id;
	}

	// maybe create a new page
	auto storage = &self->storage;
	if (unlikely(!storage->current || storage->current->position_last == self->page_rows))
	{
		storage_add(storage);

		// prepare the page bitmap
		auto current = storage->current;
		current->position = storage->size;
		memset(current->data, 0, self->page_bitmap);
	}
	auto page     = storage->current;
	auto page_row = page->position_last;
	auto page_id  = page->id.id_page;

	// mark as used
	flat_set(self, page_id, page_row, true);
	page->position_last++;

	// set row meta
	auto row = flat_row(self, page_id, page_row);
	row->row_page   = row_page;
	row->row_offset = row_offset;
	row->padding    = 0;

	// set id
	id = page_id * self->page_rows + page_row;
	return id;
}

void
flat_remove(Flat* self, uint32_t id)
{
	auto page_id  = id / self->page_rows;
	auto page_row = id % self->page_rows;
	auto row = flat_row(self, page_id, page_row);

	// mark row as free
	flat_set(self, page_id, page_row, false);

	// update free list
	row->row_next = self->header->list_free;
	self->header->list_free = id;
	self->storage.meta->changed = true;
}
