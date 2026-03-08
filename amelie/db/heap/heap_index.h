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
heap_index(Heap* self, Keys* keys, Collection* col)
{
	collection_reset(col);

	// collect rows
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, self, NULL);
	for (; heap_iterator_has(&it); heap_iterator_next(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		auto row   = heap_iterator_at(&it);
		if (chunk->is_shadow_free)
			continue;
		collection_add(col, row);
	}

	// sort
	collection_sort(col, keys);
}
