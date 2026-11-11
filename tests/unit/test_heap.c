

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
	PageMgr page_mgr;
	page_mgr_init(&page_mgr);
	defer(page_mgr_free, &page_mgr);

	Uuid id;
	uuid_init(&id);

	unused(arg);
	auto heap = heap_allocate(&page_mgr, NULL, &id);
	defer(heap_free, heap);
}
