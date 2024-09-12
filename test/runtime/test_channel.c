
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>
#include <amelie_test.h>

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
	auto buf = channel_read(&channel);
	test(buf == NULL);
	channel_free(&channel);
}

void
test_channel_task(void *arg)
{
	Channel channel;
	channel_init(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_channel_main, &channel);

	auto buf = channel_read(&channel);
	test(buf);
	buf_free(buf);

	task_wait(&task);
	task_free(&task);

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
	Cond* on_complete = arg;
	auto channel = &am_self->channel;
	for (;;)
	{
		auto buf = channel_read(channel);
		auto msg = msg_of(buf);
		guard_buf(buf);
		if (msg->id == STOP)
			break;
		uint64_t value = *(uint64_t*)msg->data;
		consumer_sum += value;
		consumer_count++;
	}

	cond_signal(on_complete, 0);
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
	Cond event;
	cond_init(&event);

	Task consumer;
	task_init(&consumer);
	task_create(&consumer, "consumer", test_channel_consumer, &event);

	Task producer;
	task_init(&producer);
	task_create(&producer, "producer", test_channel_producer, &consumer.channel);

	cond_wait(&event);
	test(consumer_count == 1000);

	task_wait(&producer);
	task_free(&producer);
	task_wait(&consumer);
	task_free(&consumer);

	cond_free(&event);
}
