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
		chunk->prev        = heap_page_of(chunk_prev)->order;
		chunk->prev_offset = chunk_prev->offset;
	} else
	{
		chunk->prev        = 0;
		chunk->prev_offset = 0;
	}
}

hot static inline Row*
row_prev(Row* row, Heap* heap)
{
	auto chunk = heap_chunk_of(row);
	if (likely(! chunk->prev_offset))
		return NULL;
	return(Row*)heap_chunk_at(heap, chunk->prev, chunk->prev_offset)->data;
}

hot static inline Row*
row_visible(Row* row, Heap* heap, Snapshot* snapshot)
{
	// note: row is a head of the version chain
	for (; row; row = row_prev(row, heap))
	{
		// row created inside this snapshot (first visible version)
		if (row->snapshot == snapshot->id)
			break;

		// visible main row
		if (row->snapshot == 0 && row->tsn <= (uint64_t)snapshot->snapshot)
			break;
	}
	if (row && row->is_delete)
		row = NULL;
	return row;
}

hot static inline bool
row_unique(Row* row, Heap* heap)
{
	// note: row is a head of the version chain

	// ensure row has no duplicates with the same snapshot id
	for (auto a = row; a; a = row_prev(a, heap))
	{
		for (auto b = row; b; b = row_prev(b, heap))
			if (a != b && a->snapshot == b->snapshot)
				return false;
	}
	return true;
}

hot static inline void
row_gc(Row* row, Heap* heap, Snapshot* snapshot, uint64_t tsn)
{
	// note: row is a head of the version chain
	auto head = row;

	// iterate row->prev
	row = row_prev(row, heap);
	while (row)
	{
		auto prev = row_prev(row, heap);
		if (row->snapshot == snapshot->id &&
		    (row->tsn == tsn ||
		     row->tsn >  (uint64_t)snapshot->snapshot_max))
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
