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
row_allocate(Heap* heap, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)heap_allocate(heap, size);
	row_init(self, size_factor, size);
	self->is_heap = true;
	return self;
}

hot static inline Row*
row_allocate_buf(Buf* buf, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)buf_emplace(buf, size);
	row_init(self, size_factor, size);
	return self;
}

always_inline static inline void
row_free(Heap* heap, Row* row)
{
	assert(row->is_heap);
	heap_release(heap, row);
}

hot static inline Row*
row_copy(Heap* heap, Row* self)
{
	auto size = row_size(self);
	auto row  = (Row*)heap_allocate(heap, size);
	memcpy(row, self, size);
	row->is_heap = true;
	return row;
}

always_inline hot static inline uint64_t
row_tsn(Row* self)
{
	if (self->is_heap)
		return chunk_of(self)->tsn;
	return 0;
}

always_inline hot static inline void
row_tsn_set(Row* self, uint64_t tsn)
{
	assert(self->is_heap);
	chunk_of(self)->tsn = tsn;
}

always_inline hot static inline void
row_tsn_follow(Row* self, Heap* heap, uint64_t tsn)
{
	row_tsn_set(self, tsn);
	heap_tsn_follow(heap, tsn);
}

Row* row_alter_add(Heap*, Row*, Columns*);
Row* row_alter_drop(Heap*, Row*, Columns*, Column*);
