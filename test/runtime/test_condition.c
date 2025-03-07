
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

#include <amelie.h>
#include <amelie_cli.h>
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
