

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

static int total  = 0;
static int active = 0;

static void
test_lock_main0(void* arg)
{
	unused(arg);
	auto l = lock_catalog(LOCK_EXCLUSIVE);
	defer(unlock, l);

	active++;

	for (auto i = 0; i < 1234; i++)
		total += 1;
	total++;
	for (auto i = 0; i < 1234; i++)
		total -= 1;

	active--;
	test(! active);
}

void
test_lock0(void* arg)
{
	unused(arg);
	total  = 0;
	active = 0;

	Task task[10];
	for (auto i = 0; i < 10; i++)
	{
		task_init(&task[i]);
		task_create(&task[i], "test", test_lock_main0, NULL);
	}

	for (auto i = 0; i < 10; i++)
	{
		task_wait(&task[i]);
		task_free(&task[i]);
	}

	test(total == 10);
}

void
test_lock1(void* arg)
{
	unused(arg);
	total  = 0;
	active = 0;

	auto l = lock_catalog(LOCK_SHARED);

	Task task[10];
	for (auto i = 0; i < 10; i++)
	{
		task_init(&task[i]);
		task_create(&task[i], "test", test_lock_main0, NULL);
	}

	coroutine_yield();
	test(total  == 0);
	test(active == 0);
	unlock(l);

	for (auto i = 0; i < 10; i++)
	{
		task_wait(&task[i]);
		task_free(&task[i]);
	}

	test(total == 10);
}

void
test_lock2(void* arg)
{
	unused(arg);
	total  = 0;
	active = 0;

	auto l = lock_catalog(LOCK_EXCLUSIVE);

	Task task[10];
	for (auto i = 0; i < 10; i++)
	{
		task_init(&task[i]);
		task_create(&task[i], "test", test_lock_main0, NULL);
	}

	coroutine_yield();
	test(total  == 0);
	test(active == 0);
	unlock(l);

	for (auto i = 0; i < 10; i++)
	{
		task_wait(&task[i]);
		task_free(&task[i]);
	}

	test(total == 10);
}

void
test_lock3(void* arg)
{
	unused(arg);
	total  = 0;
	active = 0;

	// reentrant
	auto l0 = lock_catalog(LOCK_SHARED);
	auto l1 = lock_catalog(LOCK_SHARED);
	test(l0 == l1);

	unlock(l1);
	unlock(l0);

	l0 = lock_catalog(LOCK_EXCLUSIVE);
	l1 = lock_catalog(LOCK_EXCLUSIVE);
	test(l0 == l1);

	unlock(l1);
	unlock(l0);
}
