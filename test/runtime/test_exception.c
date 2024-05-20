
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata.h>
#include <sonata_test.h>

void
test_exception0(void* arg)
{
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
	Exception e;
	if (enter(&e)) {
		nested();
	}
	if(leave(&e)) {
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
		guard(on_guard, &run);
	}
	test(run);
}

void
test_guard1(void* arg)
{
	bool run = false;
	{
		guard(on_guard, &run);
		unguard();
	}
	test(! run);
}

void
test_guard2(void* arg)
{
	// exception
	bool run = false;
	Exception e;
	if (enter(&e)) {
		guard(on_guard, &run);
		error("test");
	}
	if (leave(&e))
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
		guard(on_guard_int, &run);
		i++;
	}
	test(run == 10);

	run = 0;
	i = 0;
	while (i < 10) {
		guard(on_guard_int, &run);
		unguard();
		i++;
	}
	test(run == 0);
}
