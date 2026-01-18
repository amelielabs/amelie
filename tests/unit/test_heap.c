

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

#include <amelie_test>

void
test_heap(void* arg)
{
	unused(arg);

	auto heap = heap_allocate();
	defer(heap_free, heap);
	heap_create(heap);

	auto ptr = heap_add(heap, 903);
	memset(ptr, 'X', 903);
	heap_remove(heap, ptr);

	ptr = heap_add(heap, 1432);
	memset(ptr, 'X', 1432);
	heap_remove(heap, ptr);
}
