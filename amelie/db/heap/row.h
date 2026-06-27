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

	if (! flat_mgr->list_count)
		return;

	auto pos = (Flat**)flat_mgr->list.start;
	auto end = (Flat**)flat_mgr->list.position;
	for (; pos < end; pos++)
	{
		auto flat = *pos;
		auto ref  = row_at(row, flat->column->order);
		if (ref)
			flat_remove(flat, *(uint32_t*)ref);
	}
}

hot static inline void
row_filter(FlatMgr* flat_mgr, Row* row, bool filter)
{
	if (! flat_mgr->list_count)
		return;

	auto pos = (Flat**)flat_mgr->list.start;
	auto end = (Flat**)flat_mgr->list.position;
	for (; pos < end; pos++)
	{
		auto flat = *pos;
		auto ref  = row_at(row, flat->column->order);
		if (ref)
			flat_row_at(flat, *(uint32_t*)ref)->filter = filter;
	}
}

Row* row_copykey(Heap*, Row*, Columns*);
