
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone.h>
#include <monotone_test.h>

void
test_exception0(void* arg)
{
	Exception e;
	if (try(&e)) {
	} else
	{
		test(0);
	}
	test(! catch(&e));
}

void
test_exception1(void* arg)
{
	Exception e;
	if (try(&e)) {
		error("test");
	} else
	{
		test(1);
	}
	if(catch(&e)) {
		test(1);
	}
}

static void
nested(void)
{
	Exception e;
	if (try(&e)) {
		error("test");
	}
	if(catch(&e)) {
		rethrow();
	}
}

void
test_exception2(void* arg)
{
	Exception e;
	if (try(&e)) {
		nested();
	}
	if(catch(&e)) {
		test(1);
	}
}

static void on_guard(void* arg)
{
	*(bool*)arg = true;
}

void
test_guard0(void* arg)
{
	// on block exit
	bool run = false;
	{
		guard(guard_test, on_guard, &run);
	}
	test(run);
}

void
test_guard1(void* arg)
{
	bool run = false;
	{
		guard(guard_test, on_guard, &run);
		unguard(&guard_test);
	}
	test(! run);
}

void
test_guard2(void* arg)
{
	// exception
	bool run = false;
	Exception e;
	if (try(&e)) {
		guard(guard_test, on_guard, &run);
		error("test");
	}
	if(catch(&e))
		test(true);
	test(run);
}

static void on_guard_int(void* arg)
{
	*(int*)arg += 1;
}

void
test_guard3(void* arg)
{
	int run = 0;
	int i = 0;
	while (i < 10) {
		guard(guard_test, on_guard_int, &run);
		i++;
	}
	test(run == 10);

	run = 0;
	i = 0;
	while (i < 10) {
		guard(guard_test, on_guard_int, &run);
		unguard(&guard_test);
		i++;
	}
	test(run == 0);
}
