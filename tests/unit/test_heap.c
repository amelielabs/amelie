

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
}
