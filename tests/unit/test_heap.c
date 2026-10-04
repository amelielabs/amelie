

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
	Id id;
	id_init(&id);

	auto heap = heap_allocate(&id);
	defer(heap_free, heap);
}
