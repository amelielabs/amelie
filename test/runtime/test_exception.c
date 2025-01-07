
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

#include <amelie_private.h>
#include <amelie_test.h>

void
test_exception0(void* arg)
{
	unused(arg);
	Exception e;
	if (enter(&e)) {
	} else
	{
		test(0);
	}
	test(! leave(&e));
}

void
test_exception1(void* arg)
{
	unused(arg);
	Exception e;
	if (enter(&e)) {
		error("test");
	} else
	{
		test(1);
	}
	if(leave(&e)) {
		test(1);
	}
}

static void
nested(void)
{
	Exception e;
	if (enter(&e)) {
		error("test");
	}
	if(leave(&e)) {
		rethrow();
	}
}

void
test_exception2(void* arg)
{
	unused(arg);
	Exception e;
	if (enter(&e)) {
		nested();
	}
	if(leave(&e)) {
		test(1);
	}
}

static void on_defer_int(void* arg)
{
	*(int*)arg += 1;
}

void
test_defer0(void* arg)
{
	unused(arg);
	// on block exit
	int run = 0;
	{
		defer(on_defer_int, &run);
	}
	test(run);
}

void
test_defer1(void* arg)
{
	unused(arg);
	int run = 0;
	{
		defer(on_defer_int, &run);
		undefer();
	}
	test(! run);
}

static int _run = 0;

void
test_defer2(void* arg)
{
	unused(arg);
	// exception
	Exception e;
	if (enter(&e)) {
		defer(on_defer_int, &_run);
		error("test");
	}
	if (leave(&e))
		test(true);
	test(_run);
}

void
test_defer3(void* arg)
{
	unused(arg);
	int run = 0;
	int i = 0;
	while (i < 10) {
		defer(on_defer_int, &run);
		i++;
	}
	test(run == 10);

	run = 0;
	i = 0;
	while (i < 10) {
		defer(on_defer_int, &run);
		undefer();
		i++;
	}
	test(run == 0);
}
