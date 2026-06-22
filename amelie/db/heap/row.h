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
row_allocate(Heap* heap, uint64_t tsn, uint32_t snapshot, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)heap_add(heap, size);
	row_init(self, tsn, snapshot, columns, size_factor, size);
	heap_follow(heap, snapshot);
	return self;
}

hot static inline Row*
row_allocate_buf(Buf* buf, int columns, int data_size)
{
	int  size_factor;
	auto size = row_measure(columns, data_size, &size_factor);
	auto self = (Row*)buf_emplace(buf, size);
	row_init(self, 0, 0, columns, size_factor, size);
	return self;
}

hot static inline void
row_free(Heap* heap, FlatMgr* flat_mgr, Row* row)
{
	heap_remove(heap, row);

	unused(flat_mgr);
#if 0
	if (! flat_mgr->list_count)
		return;
	list_foreach(&flat_mgr->list)
	{
		auto flat = list_at(Flat, link);
		auto ref = row_at(row, flat->column->order);
		if (! ref)
			continue;
		flat_remove(flat, *(uint32_t*)ref);
	}
#endif
}

hot static inline Row*
row_copy(Heap* heap, Row* self)
{
	auto size = row_size(self);
	auto row  = (Row*)heap_add(heap, size);
	memcpy(row, self, size);
	heap_follow(heap, row->snapshot);
	return row;
}
