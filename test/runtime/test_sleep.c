
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo.h>
#include <indigo_test.h>

static int sleep_complete = 0;

static void
test_sleep_main(void* arg)
{
	coroutine_sleep(rand() % 100);
	sleep_complete++;
}

void
test_sleep(void* arg)
{
	uint64_t id[100];

	int i = 0;
	while (i < 100)
	{
		id[i] = coroutine_create(test_sleep_main, NULL);
		i++;
	}

	i = 0;
	while (i < 100)
	{
		coroutine_wait(id[i]);
		i++;
	}

	test(sleep_complete == 100);
}
