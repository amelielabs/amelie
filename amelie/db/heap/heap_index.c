
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>

hot static int
heap_index_cmp(const void* p1, const void* p2, void* arg)
{
	return compare(arg, *(Row**)p1, *(Row**)p2);
}

void
heap_index(Heap* self, Keys* keys, Buf* index)
{
	buf_reset(index);
	int count = 0;

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
		buf_write(index, &row, sizeof(Row**));
		count++;
	}

	// sort
	qsort_r(index->start, count, sizeof(Row*),
	        heap_index_cmp, keys);
}
