

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

#include <amelie_test.h>

void
test_heap(void* arg)
{
	unused(arg);

	Heap heap;
	heap_init(&heap);
	defer(heap_free, &heap);
	heap_create(&heap);

	auto ptr = heap_allocate(&heap, 903);
	memset(ptr, 'X', 903);
	heap_release(&heap, ptr);

	ptr = heap_allocate(&heap, 1432);
	memset(ptr, 'X', 1432);
	heap_release(&heap, ptr);
}
