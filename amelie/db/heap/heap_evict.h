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

static inline void
heap_evict(Heap* self, Collection* col, uint64_t up_to)
{
	collection_reset(col);

	// collect cold rows
	uint32_t at = self->header->lru_tail;
	uint32_t at_offset = self->header->lru_tail_offset;
	while (at)
	{
		auto chunk = heap_chunk_at(self, at, at_offset);
		auto row   = (Row*)chunk->data;
		if (col->size + row_size(row) >= up_to)
			break;
		collection_add(col, row);
		chunk->is_evicted = true;
		at = chunk->prev;
		at_offset = chunk->prev_offset;
	}
}
