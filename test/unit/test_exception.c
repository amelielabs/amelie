
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
test_exception0(void* arg)
{
	unused(arg);
	bool on_error = error_catch( true );
	test(! on_error);
}

void
test_exception1(void* arg)
{
	unused(arg);
	bool on_error = error_catch( error("test") );
	test(on_error);

	on_error = error_catch( error_catch( error("test") ) );
	test(! on_error);

	on_error = error_catch( error_catch( error_catch( error("test") ) ) );
	test(! on_error);
}

static void
nested(void)
{
	bool on_error = error_catch( error("test") );
	test(on_error);
	if (on_error)
		rethrow();
}

void
test_exception2(void* arg)
{
	unused(arg);
	bool on_error = error_catch( nested() );
	test(on_error);
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
		errdefer(on_defer_int, &run);
	}
	test(! run);
}

static int _run = 0;

void
test_defer2(void* arg)
{
	unused(arg);
	auto on_error = error_catch
	(
		defer(on_defer_int, &_run);
		error("test");
	);
	if (on_error)
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
		errdefer(on_defer_int, &run);
		i++;
	}
	test(run == 0);
}
