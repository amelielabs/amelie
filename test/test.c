
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include "test.h"

extern void test_task_create(void);
extern void test_task_args(void);
extern void test_task_status(void);
extern void test_exception0(void*);
extern void test_exception1(void*);
extern void test_exception2(void*);
extern void test_guard0(void*);
extern void test_guard1(void*);
extern void test_guard2(void*);
extern void test_guard3(void*);
extern void test_palloc(void*);
extern void test_buf(void*);
extern void test_buf_reserve(void*);
extern void test_buf_write(void*);
extern void test_buf_overwrite(void*);
extern void test_msg(void*);
extern void test_coroutine_create(void*);
extern void test_coroutine_context_switch(void*);
extern void test_event(void*);
extern void test_event_timeout(void*);
extern void test_event_parent(void*);
extern void test_condition(void*);
extern void test_condition_task(void*);
extern void test_condition_task_timeout(void*);
extern void test_channel_read_empty(void*);
extern void test_channel(void*);
extern void test_channel_task(void*);
extern void test_channel_task_detached(void*);
extern void test_channel_task_timeout(void*);
extern void test_channel_producer_consumer(void*);
extern void test_rpc(void*);
extern void test_rpc_execute(void*);
extern void test_rpc_execute_error(void*);
extern void test_rpc_benchmark(void*);
extern void test_rpc_mutex_benchmark(void*);
extern void test_cancel_create(void*);
extern void test_cancel(void*);
extern void test_cancel_pause(void*);
extern void test_cancel_condition(void*);
extern void test_cancel_channel_pause(void*);
extern void test_lock_write(void*);
extern void test_lock_write_readers(void*);
extern void test_lock_read0(void*);
extern void test_lock_read1(void*);
extern void test_sleep(void*);
extern void test_blob(void*);
extern void test_tcp_10(void*);
extern void test_tcp_100(void*);

static inline void
test_runner(char* name, MainFunction main)
{
	fprintf(stdout, "%s: ", name);

	BufCache buf_cache;
	buf_cache_init(&buf_cache);

	Task task;
	task_init(&task, &buf_cache);

	int rc;
	rc = task_create_nothrow(&task, "test", main, NULL, NULL, NULL, NULL);
	test(rc == 0);

	task_wait(&task);
	task_free(&task);
	buf_cache_free(&buf_cache);

	fprintf(stdout, "ok\n");
}

int
main(int argc, char* argv[])
{
	test_run_function(test_task_create);
	test_run_function(test_task_args);
	test_run_function(test_task_status);

	test_run(test_exception0);
	test_run(test_exception1);
	test_run(test_exception2);
	test_run(test_guard0);
	test_run(test_guard1);
	test_run(test_guard2);
	test_run(test_guard3);
	test_run(test_palloc);
	test_run(test_buf);
	test_run(test_buf_reserve);
	test_run(test_buf_write);
	test_run(test_buf_overwrite);
	test_run(test_msg);
	test_run(test_coroutine_create);
	test_run(test_coroutine_context_switch);
	test_run(test_event);
	test_run(test_event_timeout);
	test_run(test_event_parent);
	test_run(test_condition);
	test_run(test_condition_task);
	test_run(test_condition_task_timeout);
	test_run(test_channel_read_empty);
	test_run(test_channel);
	test_run(test_channel_task);
	test_run(test_channel_task_detached);
	test_run(test_channel_task_timeout);
	test_run(test_channel_producer_consumer);
	test_run(test_rpc);
	test_run(test_rpc_execute);
	test_run(test_rpc_execute_error);
	//test_run(test_rpc_benchmark);
	//test_run(test_rpc_mutex_benchmark);
	test_run(test_cancel_create);
	test_run(test_cancel);
	test_run(test_cancel_pause);
	test_run(test_cancel_condition);
	test_run(test_cancel_channel_pause);
	test_run(test_lock_write);
	test_run(test_lock_write_readers);
	test_run(test_lock_read0);
	test_run(test_lock_read1);
	test_run(test_sleep);
	test_run(test_blob);
	test_run(test_tcp_10);
	test_run(test_tcp_100);
	return 0;
}
