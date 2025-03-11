
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

static int called = 0;

void
test_task_create_main(void* arg)
{
	unused(arg);
	called++;
}

void test_task_create(void)
{
	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, "test", test_task_create_main,
	                         NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);

	task_wait(&task);
	task_free(&task);

	test(called == 1);
}

void
test_task_args_main(void* arg)
{
	test( *(int*)arg == 123 );
	test( *(int*)am_task->main_arg_global == 321 );
}

void test_task_args(void)
{
	int arg = 123;
	int arg_global = 321;

	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, "test", test_task_args_main,
	                         &arg, &arg_global, NULL, NULL, NULL);
	test(rc == 0);

	task_wait(&task);
	task_free(&task);

	test(called == 1);
}

void
test_task_status_main(void* arg)
{
	unused(arg);
	cond_signal(&am_task->status, 123);
}

void test_task_status(void)
{
	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, "test", test_task_status_main,
	                         NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);

	test(cond_wait(&task.status) == 123);

	task_wait(&task);
	task_free(&task);
}
