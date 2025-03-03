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
	return self;
}

hot static inline Row*
row_allocate_buf(Buf* buf, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)buf_claim(buf, size);
	row_init(self, size_factor, size);
	return self;
}

always_inline static inline void
row_free(Heap* heap, Row* row)
{
	heap_release(heap, row);
}

hot static inline Row*
row_copy(Heap* heap, Row* self)
{
	auto size = row_size(self);
	auto row  = heap_allocate(heap, size);
	memcpy(row, self, size);
	return row;
}

Row* row_alter_add(Heap*, Row*, Columns*);
Row* row_alter_drop(Heap*, Row*, Columns*, Column*);
