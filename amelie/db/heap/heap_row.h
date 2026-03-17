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

hot static inline Row*
row_allocate(Heap* heap, uint64_t tsn, uint32_t branch, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)heap_add(heap, size);
	row_init(self, branch, columns, size_factor, size);
	heap_chunk_of(self)->tsn = tsn;
	return self;
}

hot static inline Row*
row_allocate_buf(Buf* buf, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)buf_emplace(buf, size);
	row_init(self, 0, columns, size_factor, size);
	return self;
}

always_inline static inline void
row_free(Heap* heap, Row* row)
{
	heap_remove(heap, row);
}

hot static inline Row*
row_copy(Heap* heap, Row* self)
{
	auto size = row_size(self);
	auto row  = (Row*)heap_add(heap, size);
	memcpy(row, self, size);
	return row;
}

always_inline static inline uint64_t
row_tsn(Row* self)
{
	return heap_chunk_of(self)->tsn;
}

hot static inline Row*
row_visible(Row* row, Heap* heap, Branch* branch)
{
	// row is a head of version chain
	while (row)
	{
		// update by this branch
		if (row->branch == branch->id)
			break;

		// update by one of the derived parent branches
		auto chunk = heap_chunk_of(row);

		auto parent = branch->parent;
		for (; parent; parent = parent->parent)
			if (row->branch == parent->id && chunk->tsn <= (uint64_t)parent->snapshot)
				break;
		if (! chunk->prev_offset)
			return NULL;

		// previous version
		row = (Row*)heap_chunk_at(heap, chunk->prev, chunk->prev_offset)->data;
	}
	return row;
}
