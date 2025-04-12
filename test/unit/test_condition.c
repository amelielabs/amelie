
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

static int cond_value = 0;

static void
test_condition_main(void* arg)
{
	Event* cond = arg;
	event_signal(cond);
	cond_value = 123;
}

void
test_condition(void* arg)
{
	unused(arg);
	Event cond;
	event_init(&cond);
	event_attach(&cond);

	uint64_t id;
	id = coroutine_create(test_condition_main, &cond);
	test( id != 0 );

	bool timeout = event_wait(&cond, -1);
	test(! timeout);
	test(cond_value == 123);

	coroutine_wait(id);
	event_detach(&cond);
}

void
test_condition_task(void* arg)
{
	unused(arg);
	cond_value = 0;

	Event cond;
	event_init(&cond);
	event_attach(&cond);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_condition_main, &cond);

	bool timeout = event_wait(&cond, -1);
	test(! timeout);
	test(cond_value == 123);

	task_wait(&task);
	task_free(&task);

	event_detach(&cond);
}

static void
test_condition_timeout_main(void* arg)
{
	Event* cond = arg;
	coroutine_sleep(100);
	event_signal(cond);
	cond_value = 123;
}

void
test_condition_task_timeout(void* arg)
{
	unused(arg);
	cond_value = 0;

	Event cond;
	event_init(&cond);
	event_attach(&cond);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_condition_timeout_main, &cond);

	bool timeout = event_wait(&cond, 10);
	test(timeout);

	timeout = event_wait(&cond, -1);
	test(!timeout);

	test(cond_value == 123);

	task_wait(&task);
	task_free(&task);

	event_detach(&cond);
}

static void
test_condition_n_main(void* arg)
{
	Event* cond = arg;
	event_signal(cond);
}

void
test_condition_n_task(void* arg)
{
	unused(arg);

	const auto count = 10;
	Event cond[count];
	Task  cond_task[count];
	for (auto i = 0; i < count; i++)
	{
		event_init(&cond[i]);
		event_attach(&cond[i]);
		task_init(&cond_task[i]);
		task_create(&cond_task[i], "test", test_condition_n_main, &cond[i]);
	}

	for (auto i = 0; i < count; i++)
	{
		event_wait(&cond[i], -1);
		task_wait(&cond_task[i]);
		task_free(&cond_task[i]);
		event_detach(&cond[i]);
	}
}

static void
test_condition_n_main2(void* arg)
{
	Event* cond = arg;
	event_signal(&cond[0]);
	event_signal(&cond[1]);
	event_signal(&cond[2]);
	event_signal(&cond[3]);
	event_signal(&cond[4]);
	event_signal(&cond[5]);
	event_signal(&cond[6]);
	event_signal(&cond[7]);
	event_signal(&cond[8]);
	event_signal(&cond[9]);
}

void
test_condition_n_task2(void* arg)
{
	unused(arg);

	const auto count = 40;
	Event cond[count * 10];
	Task  cond_task[count];

	for (auto i = 0; i < count * 10; i++)
	{
		event_init(&cond[i]);
		event_attach(&cond[i]);
	}

	auto j = 0;
	for (auto i = 0; i < count; i++)
	{
		task_init(&cond_task[i]);
		task_create(&cond_task[i], "test", test_condition_n_main2, &cond[j]);
		j += 10;
	}

	for (auto i = 0; i < count * 10; i++)
	{
		event_wait(&cond[i], -1);
		event_detach(&cond[i]);
	}

	for (auto i = 0; i < count; i++)
	{
		task_wait(&cond_task[i]);
		task_free(&cond_task[i]);
	}
}
