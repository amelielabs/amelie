
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

static uint64_t coroutine_called = -1;

static void
test_coroutine_main(void* arg)
{
	unused(arg);
	coroutine_called = am_self()->id;
}

void
test_coroutine_create(void* arg)
{
	unused(arg);
	uint64_t id;
	id = coroutine_create(test_coroutine_main, NULL);
	test( id != 0 );
	coroutine_wait(id);
	test(coroutine_called == id);
}

static void
test_coroutine_context_switch_main(void* arg)
{
	int* csw = arg;
	while (*csw < 100000)
	{
		coroutine_sleep(0);
		(*csw)++;
	}
}

void
test_coroutine_context_switch(void* arg)
{
	unused(arg);
	int csw = 0;
	uint64_t id;
	id = coroutine_create(test_coroutine_context_switch_main, &csw);
	test( id != 0 );
	coroutine_wait(id);
	test(csw == 100000);
}
