
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata.h>
#include <sonata_test.h>

static int cond_value = 0;

static void
test_condition_main(void *arg)
{
	Condition* cond = arg;
	condition_signal(cond);
	cond_value = 123;
}

void
test_condition(void *arg)
{
	auto cond = condition_create();

	uint64_t id;
	id = coroutine_create(test_condition_main, cond);
	test( id != 0 );

	bool timeout = condition_wait(cond, -1);
	test(! timeout);
	test(cond_value == 123);

	coroutine_wait(id);
	condition_free(cond);
}

void
test_condition_task(void *arg)
{
	cond_value = 0;

	auto cond = condition_create();

	Task task;
	task_init(&task);
	task_create(&task, "test", test_condition_main, cond);

	bool timeout = condition_wait(cond, -1);
	test(! timeout);
	test(cond_value == 123);

	task_wait(&task);
	task_free(&task);

	condition_free(cond);
}

static void
test_condition_timeout_main(void *arg)
{
	Condition* cond = arg;
	coroutine_sleep(100);
	condition_signal(cond);
	cond_value = 123;
}

void
test_condition_task_timeout(void *arg)
{
	cond_value = 0;

	auto cond = condition_create();

	Task task;
	task_init(&task);
	task_create(&task, "test", test_condition_timeout_main, cond);

	bool timeout = condition_wait(cond, 10);
	test(timeout);

	timeout = condition_wait(cond, -1);
	test(!timeout);

	test(cond_value == 123);

	task_wait(&task);
	task_free(&task);

	condition_free(cond);
}
