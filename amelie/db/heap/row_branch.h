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

hot static inline void
row_prev_set(Row* row, Row* prev)
{
	// row->prev = prev
	auto chunk = heap_chunk_of(row);
	auto chunk_prev = heap_chunk_of(prev);
	chunk->prev           = heap_page_of(chunk_prev)->order;
	chunk->prev_offset    = chunk_prev->offset;
	chunk->is_shadow_prev = chunk_prev->is_shadow;
}

hot static inline Row*
row_prev(Row* row, Heap* heap)
{
	auto chunk = heap_chunk_of(row);
	if (! chunk->prev_offset)
		return NULL;

	// previous version
	if (unlikely(chunk->is_shadow_prev))
		return (Row*)heap_chunk_at(heap->shadow, chunk->prev, chunk->prev_offset)->data;

	return(Row*)heap_chunk_at(heap, chunk->prev, chunk->prev_offset)->data;
}

hot static inline Row*
row_visible(Row* row, Heap* heap, Branch* branch)
{
	// row is a head of the version chain
	for (; row; row = row_prev(row, heap))
	{
		// row created inside this branch
		if (row->branch == branch->id)
			return row;

		// row created by one of the branch parents
		auto parent = branch->parent;
		for (; parent; parent = parent->parent)
		{
			if (row->branch == parent->id && row->tsn <= (uint64_t)branch->snapshot)
				return row;
		}
	}
	return NULL;
}
