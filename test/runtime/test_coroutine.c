
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie.h>
#include <amelie_test.h>

static int coroutine_called = -1;

static void
test_coroutine_main(void* arg)
{
	(void)arg;
	coroutine_called = am_self()->id;
}

void
test_coroutine_create(void* arg)
{
	uint64_t id;
	id = coroutine_create(test_coroutine_main, NULL);
	test( id != 0 );
	coroutine_wait(id);
	test(coroutine_called == id);
}

static void
test_coroutine_context_switch_main(void *arg)
{
	int* csw = arg;
	while (*csw < 100000)
	{
		coroutine_sleep(0);
		(*csw)++;
	}
}

void
test_coroutine_context_switch(void *arg)
{
	int csw = 0;
	uint64_t id;
	id = coroutine_create(test_coroutine_context_switch_main, &csw);
	test( id != 0 );
	coroutine_wait(id);
	test(csw == 100000);
}
