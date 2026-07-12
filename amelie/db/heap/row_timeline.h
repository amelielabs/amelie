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
		chunk->prev        = heap_page_of(chunk_prev)->id;
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

hot static inline bool
row_unique(Row* row, Heap* heap)
{
	// note: row is the head of the version chain

	// ensure row has no duplicates with the same timeline
	for (auto a = row; a; a = row_prev(a, heap))
	{
		for (auto b = row; b; b = row_prev(b, heap))
			if (a != b && a->timeline == b->timeline)
				return false;
	}
	return true;
}

hot static inline Row*
row_visible(Row* row, Heap* heap, Timeline* timeline)
{
	// note: row is the head of the version chain
	if (timeline->main)
	{
		// first main row
		for (; row; row = row_prev(row, heap))
			if (row->main)
				break;
	} else
	{
		// first visible row by the clone
		for (; row; row = row_prev(row, heap))
		{
			if (row->timeline > timeline->timeline)
				continue;

			// main row (with timeline <= from clone creation)
			if (row->main)
				break;

			// created by the clone
			if (row->timeline == timeline->timeline)
				break;
		}
	}
	if (row && row->is_delete)
		row = NULL;
	return row;
}

hot static inline void
row_seal_and_gc(Row* row, Heap* heap, Flats* flats, Timeline* timeline)
{
	// note: row is the head of the version chain
	auto head = row;
	row = row_prev(row, heap);

	// mark head in the version chain for recovery
	head->head = true;

	// main
	if (timeline->main)
	{
		// cleanup duplicates not visible by clones
		while (row)
		{
			row->head = false;
			auto prev = row_prev(row, heap);
			if (row->main && row->timeline > timeline->timeline_max)
			{
				row_prev_set(head, prev);
				row_free(heap, flats, row);
			} else  {
				head = row;
			}
			row = prev;
		}
		return;
	}

	// clone
	while (row)
	{
		// cleanup duplicates from this clone
		row->head = false;
		auto prev = row_prev(row, heap);
		if (!row->main && row->timeline == timeline->timeline)
		{
			row_prev_set(head, prev);
			row_free(heap, flats, row);
		} else {
			head = row;
		}
		row = prev;
	}
}
