
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata.h>
#include <sonata_test.h>

static bool created = false;

static void
test_cancel_create_main(void* arg)
{
	created = true;
}

void
test_cancel_create(void* arg)
{
	uint64_t id;
	id = coroutine_create(test_cancel_create_main, NULL);
	test( id != 0 );
	coroutine_kill(id);
	test(! created);
}

static void
test_cancel_main(void *arg)
{
	Event* event = arg;
	event_wait(event, -1);
	test( so_self()->cancel );
}

void
test_cancel(void *arg)
{
	Event event;
	event_init(&event);

	uint64_t id;
	id = coroutine_create(test_cancel_main, &event);
	test( id != 0 );

	coroutine_sleep(0);

	coroutine_kill(id);
	test(! so_self()->cancel );
}

static bool pause_started = false;
static bool pause_handled = false;

static void
test_cancel_pause_main(void* arg)
{
	Event* event = arg;

	coroutine_cancel_pause(so_self());
	pause_started = true;

	event_wait(event, -1);
	pause_handled = true;

	/* cancellation point */
	coroutine_cancel_resume(so_self());

	/* unreach */
	test( 0 );
}

void
test_cancel_pause(void *arg)
{
	Event event;
	event_init(&event);

	uint64_t id;
	id = coroutine_create(test_cancel_pause_main, &event);
	test( id != 0 );

	coroutine_sleep(0);
	test( pause_started );

	coroutine_kill_nowait(id);
	test( pause_handled == false );

	event_signal(&event);
	coroutine_sleep(0);
	test( pause_handled );

	coroutine_wait(id);
	test(! so_self()->cancel );
}

static void
test_cancel_condition_main(void *arg)
{
	Condition* cond = arg;
	condition_wait(cond, -1);
	test( so_self()->cancel );
}

void
test_cancel_condition(void *arg)
{
	auto cond = condition_create();

	uint64_t id;
	id = coroutine_create(test_cancel_condition_main, cond);
	test( id != 0 );

	coroutine_sleep(0);

	coroutine_kill(id);
	test(! so_self()->cancel );

	condition_free(cond);
}

static void
test_cancel_channel_pause_main(void* arg)
{
	Channel* channel = arg;
	coroutine_sleep(100);
	channel_write(channel, buf_end(buf_begin())); 
}

static void
test_cancel_channel_pause_canceller(void* arg)
{
	uint64_t id = *(uint64_t*)arg;
	coroutine_kill_nowait(id);
}

void
test_cancel_channel_pause(void *arg)
{
	Channel channel;
	channel_init(&channel);
	channel_attach(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_cancel_channel_pause_main, &channel);

	uint64_t self = so_self()->id;
	uint64_t id;
	id = coroutine_create(test_cancel_channel_pause_canceller, &self);
	test( id != 0 );

	coroutine_cancel_pause(so_self());

	auto buf = channel_read(&channel, -1);
	test(buf);
	buf_free(buf);

	task_wait(&task);
	task_free(&task);

	channel_detach(&channel);
	channel_free(&channel);

	coroutine_cancel_resume(so_self()); // throw

	/* unreach */
	test( 0 );
}
