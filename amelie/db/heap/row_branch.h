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
	if (prev)
	{
		auto chunk_prev = heap_chunk_of(prev);
		chunk->prev           = heap_page_of(chunk_prev)->order;
		chunk->prev_offset    = chunk_prev->offset;
		chunk->is_shadow_prev = chunk_prev->is_shadow;
	} else
	{
		chunk->prev           = 0;
		chunk->prev_offset    = 0;
		chunk->is_shadow_prev = false;
	}
}

hot static inline Row*
row_prev(Row* row, Heap* heap, Heap* shadow)
{
	auto chunk = heap_chunk_of(row);
	if (likely(! chunk->prev_offset))
		return NULL;

	// previous version
	if (unlikely(chunk->is_shadow_prev))
		return (Row*)heap_chunk_at(shadow, chunk->prev, chunk->prev_offset)->data;

	return(Row*)heap_chunk_at(heap, chunk->prev, chunk->prev_offset)->data;
}

hot static inline Row*
row_visible(Row* row, Heap* heap, Branch* branch)
{
	// note: row is a head of the version chain
	for (; row; row = row_prev(row, heap, heap->shadow))
	{
		// row created inside this branch
		if (row->branch == branch->id)
			break;

		// row created by one of the branch parents
		auto parent = branch->parent;
		for (; parent; parent = parent->parent)
		{
			if (row->branch == parent->id && row->tsn <= (uint64_t)branch->snapshot)
				break;
		}
	}
	if (row && row->is_delete)
		row = NULL;
	return row;
}

hot static inline bool
row_unique(Row* row, Heap* heap, Heap* shadow)
{
	// note: row is a head of the version chain

	// ensure row has no duplicate with the same branch id
	for (auto a = row; a; a = row_prev(a, heap, shadow))
	{
		for (auto b = row; b; b = row_prev(b, heap, shadow))
			if (a != b && a->branch == b->branch)
				return false;
	}
	return true;
}

hot static inline void
row_gc(Row* row, Heap* heap, Branch* branch, uint64_t tsn)
{
	// note: row is a head of the version chain
	auto head = row;

	// iterate row->prev
	row = row_prev(row, heap, heap->shadow);
	while (row)
	{
		auto prev = row_prev(row, heap, heap->shadow);
		if (row->tsn == tsn ||
		    row->tsn <= (uint64_t)branch->snapshot ||
		    row->tsn >= (uint64_t)branch->snapshot_max)
		{
			// set head->prev = row->prev
			row_prev_set(head, prev);
			row_free(heap, row);
		} else {
			head = row;
		}
		row = prev;
	}
}
