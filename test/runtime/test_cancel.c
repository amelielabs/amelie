
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>
#include <amelie_test.h>

static void
test_cancel_main(void* arg)
{
	Channel* channel = arg;
	usleep(10000);
	if (am_cancelled)
		channel_write(channel, buf_end(buf_begin()));
}

void
test_cancel(void *arg)
{
	Channel channel;
	channel_init(&channel);

	Task task;
	task_init(&task);
	task_create(&task, "test", test_cancel_main, &channel);
	task_cancel(&task);

	auto buf = channel_read(&channel);
	test(buf);
	buf_free(buf);

	task_wait(&task);
	task_free(&task);

	channel_free(&channel);
}
