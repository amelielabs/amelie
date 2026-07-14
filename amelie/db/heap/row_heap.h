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
row_allocate(Heap* heap, bool main, uint32_t timeline, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = heap_add(heap, size);
	row_prepare(self, main, timeline, columns, size_factor, size);
	return self;
}

hot static inline Row*
row_allocate_buf(Buf* buf, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)buf_emplace(buf, size);
	row_init(self);
	row_prepare(self, false, 0, columns, size_factor, size);
	return self;
}

hot static inline void
row_free(Heap* heap, Flats* flats, Row* row)
{
	heap_remove(heap, row);

	if (! flats->list_count)
		return;

	auto pos = (Flat**)flats->list.start;
	auto end = (Flat**)flats->list.position;
	for (; pos < end; pos++)
	{
		auto flat = *pos;
		auto ref  = row_at(row, flat->column->order);
		if (ref)
			flat_remove(flat, *(uint32_t*)ref);
	}
}

hot static inline void
row_filter(Flats* flats, Row* row, bool filter)
{
	if (! flats->list_count)
		return;

	auto pos = (Flat**)flats->list.start;
	auto end = (Flat**)flats->list.position;
	for (; pos < end; pos++)
	{
		auto flat = *pos;
		auto ref  = row_at(row, flat->column->order);
		if (ref)
			flat_set_at(flat, *(uint32_t*)ref, !filter);
	}
}

Row* row_copykey(Heap*, Row*, Columns*);
