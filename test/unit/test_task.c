
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

static void
test_task_create_main(void* arg)
{
	unused(arg);
	called++;
}

void test_task_create(void)
{
	called = 0;

	Task task;
	task_init(&task);
	int rc;
	rc = task_create_nothrow(&task, "test", test_task_create_main,
	                         NULL, NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);
	task_wait(&task);
	task_free(&task);

	test(called == 1);
}

static void
test_task_args_main(void* arg)
{
	test( *(int*)arg == 123 );
	test( *(int*)am_task->main_arg_runtime == 321 );
}

void test_task_args(void)
{
	int arg = 123;
	int arg_runtime = 321;

	Task task;
	task_init(&task);
	int rc;
	rc = task_create_nothrow(&task, "test", test_task_args_main,
	                         &arg, &arg_runtime, NULL, NULL, NULL, NULL);
	test(rc == 0);
	task_wait(&task);
	task_free(&task);
}

static void
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
	                         NULL, NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);

	test(cond_wait(&task.status) == 123);

	task_wait(&task);
	task_free(&task);
}

void test_task_create_native(void)
{
	called = 0;

	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, "test", NULL, NULL, NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);

	task_execute(&task, test_task_create_main, NULL);
	test(called == 1);

	task_execute(&task, test_task_create_main, NULL);
	test(called == 2);

	task_execute(&task, test_task_create_main, NULL);
	test(called == 3);

	task_free(&task);
}

static void
test_task_native_error_main(void* arg)
{
	unused(arg);
	called++;
	error("_test");
}

void test_task_native_error(void)
{
	called = 0;

	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, "test", NULL, NULL, NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);

	task_execute(&task, test_task_native_error_main, NULL);
	test(called == 1);

	task_free(&task);
}

static void
test_task_native_error_main2(void* arg)
{
	unused(arg);
	called++;
	auto on_error = error_catch(
		error("_test");
	);
	*(int*)arg = on_error;
}

void test_task_native_error2(void)
{
	called = 0;

	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, "test", NULL, NULL, NULL, NULL, NULL, NULL, NULL);
	test(rc == 0);

	int on_error = 0;
	task_execute(&task, test_task_native_error_main2, &on_error);
	test(called == 1);
	test(on_error);

	task_free(&task);
}
