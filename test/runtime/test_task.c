
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone.h>
#include <monotone_test.h>

static int called = 0;

void
test_task_create_main(void* arg)
{
	called++;
}

void test_task_create(void)
{
	BufCache buf_cache;
	buf_cache_init(&buf_cache);

	Task task;
	task_init(&task, &buf_cache);

	int rc;
	rc = task_create_nothrow(&task, "test", test_task_create_main,
	                         NULL, NULL, NULL, NULL);
	test(rc == 0);

	task_wait(&task);
	task_free(&task);

	test(called == 1);

	buf_cache_free(&buf_cache);
}

void
test_task_args_main(void* arg)
{
	test( *(int*)arg == 123 );
	test( *(int*)mn_task->main_arg_global == 321 );
}

void test_task_args(void)
{
	int arg = 123;
	int arg_global = 321;

	BufCache buf_cache;
	buf_cache_init(&buf_cache);

	Task task;
	task_init(&task, &buf_cache);

	int rc;
	rc = task_create_nothrow(&task, "test", test_task_args_main,
	                         &arg, &arg_global, NULL, NULL);
	test(rc == 0);

	task_wait(&task);
	task_free(&task);

	test(called == 1);
}

void
test_task_status_main(void* arg)
{
	thread_status_set(&mn_task->thread_status, 123);
}

void test_task_status(void)
{
	BufCache buf_cache;
	buf_cache_init(&buf_cache);

	Task task;
	task_init(&task, &buf_cache);

	int rc;
	rc = task_create_nothrow(&task, "test", test_task_status_main,
	                         NULL, NULL, NULL, NULL);
	test(rc == 0);

	test(thread_status_wait(&task.thread_status) == 123);

	task_wait(&task);
	task_free(&task);
}
