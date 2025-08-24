
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

static int event_value = 0;

static void
test_event_main(void* arg)
{
	Event *event = arg;
	event_signal(event);
	event_value = 123;
}

void
test_event(void* arg)
{
	unused(arg);
	Event event;
	event_init(&event);

	uint64_t id;
	id = coroutine_create(test_event_main, &event);
	test( id != 0 );

	bool timeout = event_wait(&event, -1);
	test(! timeout);
	test(event_value == 123);

	coroutine_wait(id);
}

static void
test_event_timeout_main(void* arg)
{
	Event *event = arg;
	coroutine_sleep(100);
	event_signal(event);
	event_value = 321;
}

void
test_event_timeout(void* arg)
{
	unused(arg);
	event_value = 0;

	Event event;
	event_init(&event);

	uint64_t id;
	id = coroutine_create(test_event_timeout_main, &event);
	test( id != 0 );

	bool timeout = event_wait(&event, 10);
	test(timeout);

	timeout = event_wait(&event, -1);
	test(! timeout);
	test(event_value == 321);

	coroutine_wait(id);
}

void
test_event_parent(void* arg)
{
	unused(arg);
	event_value = 0;

	Event parent;
	event_init(&parent);

	Event event;
	event_init(&event);

	event_set_parent(&event, &parent);

	uint64_t id;
	id = coroutine_create(test_event_main, &event);
	test( id != 0 );

	bool timeout = event_wait(&parent, -1);
	test(! timeout);
	test(event_value == 123);
	coroutine_wait(id);
}
