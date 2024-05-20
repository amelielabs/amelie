
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata.h>
#include <sonata_test.h>

static void
test_channel_main(void *arg)
{
	Channel* channel = arg;
	auto buf = buf_end(buf_begin());
	channel_write(channel, buf); 
}

void
test_channel_read_empty(void* arg)
{
	Channel channel;
	channel_init(&channel);
	channel_attach(&channel);

	auto buf = channel_read(&channel, 0);
	test(buf == NULL);

	channel_detach(&channel);
	channel_free(&channel);
}

void
test_channel(void* arg)
{
	Channel channel;
	channel_init(&channel);
	channel_attach(&channel);

	uint64_t id;
	id = coroutine_create(test_channel_main, &channel);
	test( id != 0 );

	auto buf = channel_read(&channel, -1);
	test(buf);

	coroutine_wait(id);

	buf_free(buf);

	channel_detach(&channel);
	channel_free(&channel);
}

void
test_channel_task(void *arg)
{
	Channel channel;
	channel_init(&channel);
	channel_attach(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_channel_main, &channel);

	auto buf = channel_read(&channel, -1);
	test(buf);
	buf_free(buf);

	task_wait(&task);
	task_free(&task);

	channel_detach(&channel);
	channel_free(&channel);
}

static void
test_channel_timeout_main(void *arg)
{
	Channel* channel = arg;
	coroutine_sleep(100);
	channel_write(channel, buf_end(buf_begin())); 
}

void
test_channel_task_timeout(void *arg)
{
	Channel channel;
	channel_init(&channel);
	channel_attach(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_channel_timeout_main, &channel);

	auto buf = channel_read(&channel, 10);
	test(buf == NULL);

	buf = channel_read(&channel, -1);
	test(buf);
	buf_free(buf);

	task_wait(&task);
	task_free(&task);

	channel_detach(&channel);
	channel_free(&channel);
}

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
	Condition* on_complete = arg;
	auto channel = &so_task->channel; 
	for (;;)
	{
		auto buf = channel_read(channel, -1);
		auto msg = msg_of(buf);
		if (msg->id == STOP)
		{
			buf_free(buf);
			break;
		}

		uint64_t value = *(uint64_t*)msg->data;
		consumer_sum += value;
		consumer_count++;
		buf_free(buf);
	}

	condition_signal(on_complete);
}

static void
test_channel_producer(void *arg)
{
	Channel* consumer_channel = arg;
	Buf* buf;

	uint64_t i = 0;
	while (i < 1000)
	{
		buf = msg_begin(DATA);
		buf_write(buf, &i, sizeof(i));
		msg_end(buf);
		channel_write(consumer_channel, buf);
		i++;
	}

	buf = msg_begin(STOP);
	msg_end(buf);
	channel_write(consumer_channel, buf);
}

void
test_channel_producer_consumer(void *arg)
{
	auto event = condition_create();

	Task consumer;
	task_init(&consumer);
	task_create(&consumer, "consumer", test_channel_consumer, event);

	Task producer;
	task_init(&producer);
	task_create(&producer, "producer", test_channel_producer, &consumer.channel);

	bool timeout = condition_wait(event, -1);
	test(! timeout);
	test(consumer_count == 1000);

	task_wait(&producer);
	task_free(&producer);
	task_wait(&consumer);
	task_free(&consumer);

	condition_free(event);
}
