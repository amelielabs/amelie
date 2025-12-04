
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
test_palloc(void* arg)
{
	unused(arg);

	char* a = palloc(5);
	test(a != NULL);
	memcpy(a, "hello", 5);

	char* b = palloc(5);
	test(b != NULL);
	memcpy(b, "hello", 5);

	char* c = palloc(5);
	test(c != NULL);
	memcpy(c, "hello", 5);

	palloc_reset();

	c = palloc(5);
	test(c != NULL);
	memcpy(c, "hello", 5);
}
