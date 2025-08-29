
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

void
test_channel_read_empty(void* arg)
{
	unused(arg);
	Channel channel;
	channel_init(&channel, 8);
	channel_attach(&channel);

	auto msg = channel_read(&channel, 0);
	test(msg == NULL);

	channel_detach(&channel);
	channel_free(&channel);
}

static Msg _msg;

static void
test_channel_main(void *arg)
{
	Channel* channel = arg;
	channel_write(channel, &_msg);
}

void
test_channel(void* arg)
{
	msg_init(&_msg, 0);

	unused(arg);
	Channel channel;
	channel_init(&channel, 8);
	channel_attach(&channel);

	uint64_t id;
	id = coroutine_create(test_channel_main, &channel);
	test( id != 0 );

	auto msg = channel_read(&channel, -1);
	test(msg);

	coroutine_wait(id);

	channel_detach(&channel);
	channel_free(&channel);
}

void
test_channel_task(void* arg)
{
	msg_init(&_msg, 0);

	unused(arg);
	Channel channel;
	channel_init(&channel, 8);
	channel_attach(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_channel_main, &channel);

	auto msg = channel_read(&channel, -1);
	test(msg);

	task_wait(&task);
	task_free(&task);

	channel_detach(&channel);
	channel_free(&channel);
}

static void
test_channel_timeout_main(void* arg)
{
	Channel* channel = arg;
	coroutine_sleep(100);
	channel_write(channel, &_msg);
}

void
test_channel_task_timeout(void* arg)
{
	msg_init(&_msg, 0);

	unused(arg);
	Channel channel;
	channel_init(&channel, 8);
	channel_attach(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_channel_timeout_main, &channel);

	auto msg = channel_read(&channel, 10);
	test(msg == NULL);

	msg = channel_read(&channel, -1);
	test(msg);

	task_wait(&task);
	task_free(&task);

	channel_detach(&channel);
	channel_free(&channel);
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
test_channel_consumer(void *arg)
{
	Event* on_complete = arg;
	auto channel = &am_task->channel;
	for (;;)
	{
		auto msg = channel_read(channel, -1);
		if (msg->id == STOP)
			break;
		consumer_sum += ((PCEvent*)msg)->value;
		consumer_count++;
	}
	event_signal(on_complete);
}

static void
test_channel_producer(void* arg)
{
	Channel* consumer_channel = arg;
	for (int i = 0; i < 100; i++)
	{
		auto msg = &events[i];
		msg_init(&msg->msg, DATA);
		msg->value = i;
		channel_write(consumer_channel, &msg->msg);
	}
	auto stop = &events[100];
	msg_init(&stop->msg, STOP);
	channel_write(consumer_channel, &stop->msg);
}

void
test_channel_producer_consumer(void* arg)
{
	unused(arg);
	Event event;
	event_init(&event);
	event_attach(&event);

	Task consumer;
	task_init(&consumer);
	task_create(&consumer, "consumer", test_channel_consumer, &event);

	Task producer;
	task_init(&producer);
	task_create(&producer, "producer", test_channel_producer, &consumer.channel);

	bool timeout = event_wait(&event, -1);
	test(! timeout);
	test(consumer_count == 100);

	task_wait(&producer);
	task_free(&producer);
	task_wait(&consumer);
	task_free(&consumer);

	event_detach(&event);
}
