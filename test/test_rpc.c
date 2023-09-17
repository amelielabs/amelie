
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include "test.h"

static void
test_rpc_main(void *arg)
{
	auto buf = channel_read(&mn_task->channel, -1);
	auto rpc = rpc_of(buf);
	buf_free(buf);

	test(rpc->argc == 1);
	test(! memcmp(rpc_arg_ptr(rpc, 0), "hello", 5));
	rpc->rc = 123;
	rpc_done(rpc);
}

void
test_rpc(void *arg)
{
	Task task;
	task_init(&task, mn_task->buf_cache);
	task_create(&task, "test", test_rpc_main, NULL);

	int rc;
	rc = rpc(&task.channel, 0, 1, "hello");
	test(rc == 123);

	task_wait(&task);
	task_free(&task);
}

static void
test_rpc_execute_cb(Rpc* self, void *arg)
{
	test(self->argc == 1);
	test(! memcmp(rpc_arg_ptr(self, 0), "hello", 5));
	self->rc = 123;
}

static void
test_rpc_execute_main(void *arg)
{
	auto buf = channel_read(&mn_task->channel, -1);
	rpc_execute(buf, test_rpc_execute_cb, NULL);
	buf_free(buf);
}

void
test_rpc_execute(void *arg)
{
	Task task;
	task_init(&task, mn_task->buf_cache);
	task_create(&task, "test", test_rpc_execute_main, NULL);

	int rc;
	rc = rpc(&task.channel, 0, 1, "hello");
	test(rc == 123);

	task_wait(&task);
	task_free(&task);
}

static void
test_rpc_execute_error_cb(Rpc* rpc, void* arg)
{
	error("test");
}

static void
test_rpc_execute_error_main(void* arg)
{
	auto buf = channel_read(&mn_task->channel, -1);
	rpc_execute(buf, test_rpc_execute_error_cb, NULL);
	buf_free(buf);
}

void
test_rpc_execute_error(void* arg)
{
	Task task;
	task_init(&task, mn_task->buf_cache);
	task_create(&task, "test", test_rpc_execute_error_main, NULL);

	Exception e;
	if (try(&e))
		rpc(&task.channel, 0, 0);

	bool error = false;
	if (catch(&e))
		error = true;
	test(error);

	task_wait(&task);
	task_free(&task);
}
