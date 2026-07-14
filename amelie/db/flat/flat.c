
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

	// calculate how many vectors can fit the page,
	// we use 64 vectors buckets (64bit bitmap)
	auto size_page   = (512ul * 1024);
	auto size_bucket =
		sizeof(uint64_t) + // 64 vectors bitmap
		sizeof(FlatRow) * 64 +
		(64 * column->size_flat);
	auto buckets = (size_page - sizeof(Page)) / size_bucket;
	self->page_rows   = buckets * 64;
	self->page_bitmap = buckets * sizeof(uint64_t);

	self->header.list_free = UINT32_MAX;
	self->column = column;
	storage_init(&self->storage, STORAGE_FLAT, size_page);
	return self;
}

void
flat_free(Flat* self)
{
	storage_free(&self->storage);
	am_free(self);
}

size_t
flat_create(Flat* self, char* path)
{
	return storage_create(&self->storage, path, (uint8_t*)&self->header,
	                      sizeof(self->header));
}

size_t
flat_open(Flat* self, char* path)
{
	Buf meta;
	buf_init(&meta);
	defer_buf(&meta);
	auto size_file = storage_open(&self->storage, path, STORAGE_FLAT, &meta);

	// validate cdc header size
	if (unlikely(buf_size(&meta) != sizeof(FlatHeader)))
		error("storage: file '{str}' has invalid flat header", path);

	// set header
	auto header = (FlatHeader*)meta.start;
	self->header = *header;

	return size_file;
}

uint32_t
flat_add(Flat* self, int row_page, int row_offset)
{
	auto id = self->header.list_free;
	if (likely(id != UINT32_MAX))
	{
		auto page_id  = id / self->page_rows;
		auto page_row = id % self->page_rows;
		auto row = flat_row(self, page_id, page_row);

		// mark row as being used and update free list
		self->header.list_free = row->row_next;

		row->row_page   = row_page;
		row->row_offset = row_offset;
		row->padding    = 0;
		flat_set(self, page_id, page_row, true);
		return id;
	}

	// maybe create a new page
	auto storage = &self->storage;
	if (unlikely(!storage->current || storage->current->used == self->page_rows))
	{
		storage_add(storage);

		// prepare the page bitmap
		auto current = storage->current;
		current->position = storage->size_page;
		memset(current->data, 0, self->page_bitmap);
	}
	auto page = storage->current;
	auto page_row = page->used;

	// mark as used
	flat_set(self, page->id, page_row, true);
	page->used++;

	// set row meta
	auto row = flat_row(self, page->id, page_row);
	row->row_page   = row_page;
	row->row_offset = row_offset;
	row->padding    = 0;

	// set id
	id = page->id * self->page_rows + page_row;
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
	row->row_next = self->header.list_free;
	self->header.list_free = id;
}
