
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_flat.h>

Flat*
flat_allocate(Column* column)
{
	auto self = (Flat*)am_malloc(sizeof(Flat));
	auto page_size = (512ul * 1024);
	self->per_page =
		(page_size - sizeof(Page)) / (sizeof(FlatRow) + column->size_flat);
	self->column   = column;
	storage_init(&self->storage, STORAGE_FLAT, page_size);
	list_init(&self->link);
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
	return storage_create(&self->storage, path, NULL, 0);
}

size_t
flat_open(Flat* self, char* path)
{
	Buf meta;
	buf_init(&meta);
	defer_buf(&meta);
	auto size_file = storage_open(&self->storage, path, STORAGE_FLAT, &meta);
	return size_file;
}

uint32_t
flat_add(Flat* self, int row_page, int row_offset)
{
	// maybe allocate a new page
	auto storage = &self->storage;
	if (unlikely(!storage->current || storage->current->used == self->per_page))
	{
		storage_add(storage);
		storage->current->position = storage->size_page;
	}
	auto page = storage->current;

	// set id
	uint32_t id = page->id * self->per_page + page->used;
	page->used++;

	// set row meta
	auto row = flat_row(self, id);
	row->row_page   = row_page;
	row->row_offset = row_offset;
	row->used       = 1;
	row->padding    = 0;
	return id;
}

void
flat_remove(Flat* self, uint32_t id)
{
	auto row = flat_row(self, id);
	row->used = 0;
	// todo: bitmap
}
