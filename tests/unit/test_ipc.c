
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

static Msg _msg;

static void
test_ipc_main(void *arg)
{
	Task* src = arg;
	task_send(src, &_msg);
}

void
test_ipc_task(void* arg)
{
	msg_init(&_msg, 0);
	unused(arg);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_ipc_main, am_task);

	auto msg = task_recv();
	test(msg);

	task_wait(&task);
	task_free(&task);
}

typedef struct PCEvent PCEvent;

struct PCEvent
{
	Msg msg;
	int value;
};

static PCEvent events[101];

enum
{
	STOP,
	DATA
};

uint64_t consumer_sum   = 0;
uint64_t consumer_count = 0;

static void
test_ipc_consumer(void *arg)
{
	Event* on_complete = arg;
	for (;;)
	{
		auto msg = task_recv();
		if (msg->id == STOP)
			break;
		consumer_sum += ((PCEvent*)msg)->value;
		consumer_count++;
	}
	event_signal(on_complete);
}

static void
test_ipc_producer(void* arg)
{
	Task* consumer = arg;
	for (int i = 0; i < 100; i++)
	{
		auto msg = &events[i];
		msg_init(&msg->msg, DATA);
		msg->value = i;
		task_send(consumer, &msg->msg);
	}
	auto stop = &events[100];
	msg_init(&stop->msg, STOP);
	task_send(consumer, &stop->msg);
}

void
test_ipc_producer_consumer(void* arg)
{
	unused(arg);
	Event event;
	event_init(&event);
	event_attach(&event);

	Task consumer;
	task_init(&consumer);
	task_create(&consumer, "consumer", test_ipc_consumer, &event);

	Task producer;
	task_init(&producer);
	task_create(&producer, "producer", test_ipc_producer, &consumer);

	bool timeout = event_wait(&event, -1);
	test(! timeout);
	test(consumer_count == 100);

	task_wait(&producer);
	task_free(&producer);

	task_wait(&consumer);
	task_free(&consumer);
}
