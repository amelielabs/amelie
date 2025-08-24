
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

static bool created = false;

static void
test_cancel_create_main(void* arg)
{
	unused(arg);
	created = true;
}

void
test_cancel_create(void* arg)
{
	unused(arg);
	uint64_t id;
	id = coroutine_create(test_cancel_create_main, NULL);
	test( id != 0 );
	coroutine_kill(id);
	test(! created);
}

static void
test_cancel_main(void* arg)
{
	Event* event = arg;
	event_wait(event, -1);
	test( am_self()->cancel );
}

void
test_cancel(void* arg)
{
	unused(arg);
	Event event;
	event_init(&event);

	uint64_t id;
	id = coroutine_create(test_cancel_main, &event);
	test( id != 0 );

	coroutine_sleep(0);

	coroutine_kill(id);
	test(! am_self()->cancel );
}

static bool pause_started = false;
static bool pause_handled = false;

static void
test_cancel_pause_main(void* arg)
{
	Event* event = arg;

	coroutine_cancel_pause(am_self());
	pause_started = true;

	event_wait(event, -1);
	pause_handled = true;

	/* cancellation point */
	coroutine_cancel_resume(am_self());

	/* unreach */
	test( 0 );
}

void
test_cancel_pause(void* arg)
{
	unused(arg);
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
	test(! am_self()->cancel );
}

static void
test_cancel_condition_main(void* arg)
{
	Event* cond = arg;
	event_wait(cond, -1);
	test( am_self()->cancel );
}

void
test_cancel_condition(void* arg)
{
	unused(arg);
	Event cond;
	event_init(&cond);
	event_attach(&cond);

	uint64_t id;
	id = coroutine_create(test_cancel_condition_main, &cond);
	test( id != 0 );

	coroutine_sleep(0);

	coroutine_kill(id);
	test(! am_self()->cancel );

	event_detach(&cond);
}

static void
test_cancel_channel_pause_main(void* arg)
{
	Channel* channel = arg;
	coroutine_sleep(100);
	channel_write(channel, buf_create());
}

static void
test_cancel_channel_pause_canceller(void* arg)
{
	uint64_t id = *(uint64_t*)arg;
	coroutine_kill_nowait(id);
}

void
test_cancel_channel_pause(void* arg)
{
	unused(arg);
	Channel channel;
	channel_init(&channel);
	channel_attach(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_cancel_channel_pause_main, &channel);

	uint64_t self = am_self()->id;
	uint64_t id;
	id = coroutine_create(test_cancel_channel_pause_canceller, &self);
	test( id != 0 );

	coroutine_cancel_pause(am_self());

	auto buf = channel_read(&channel, -1);
	test(buf);
	buf_free(buf);

	task_wait(&task);
	task_free(&task);

	channel_detach(&channel);
	channel_free(&channel);

	coroutine_cancel_resume(am_self()); // throw

	/* unreach */
	test( 0 );
}
