
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone.h>
#include <monotone_test.h>

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

static void
test_rpc_benchmark_main(void *arg)
{
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto rpc = rpc_of(buf);
		buf_free(buf);
		stop = rpc->id == 0;
		rpc_done(rpc);
	}
}

void
test_rpc_benchmark(void *arg)
{
	Task task;
	task_init(&task, mn_task->buf_cache);
	task_create(&task, "test", test_rpc_benchmark_main, NULL);

	uint64_t time_us;
	time_start(&time_us);

	int count = 1000000;
	int i = 0;
	for(; i < count; i++)
		rpc(&task.channel, 1, 0);
	rpc(&task.channel, 0, 0); // stop

	time_end(&time_us);

	double sec = (double)time_us / 1000.0 / 1000.0;

	printf("%.2f sec (rps %.0f) ", sec, count / sec);

	task_wait(&task);
	task_free(&task);
}

static int     bench_counter  = 0;
static int     bench_counter2 = 0;
static bool    bench_stop = false;
static Mutex   bench_mutex;
static Mutex   bench_mutex2;
static CondVar bench_cond;
static CondVar bench_cond2;

static void
test_rpc_mutex_benchmark_main(void *arg)
{
	while (! bench_stop)
	{
		mutex_lock(&bench_mutex);
		if (bench_counter == bench_counter2)
			cond_var_wait(&bench_cond, &bench_mutex);
		mutex_unlock(&bench_mutex);

		mutex_lock(&bench_mutex2);
		bench_counter2++;
		cond_var_signal(&bench_cond2);
		mutex_unlock(&bench_mutex2);
	}
	test(bench_counter == bench_counter2);
}

void
test_rpc_mutex_benchmark(void *arg)
{
	Task task;
	task_init(&task, mn_task->buf_cache);
	task_create(&task, "test", test_rpc_mutex_benchmark_main, NULL);

	mutex_init(&bench_mutex);
	mutex_init(&bench_mutex2);
	cond_var_init(&bench_cond);
	cond_var_init(&bench_cond2);

	uint64_t time_us;
	time_start(&time_us);

	int count = 1000000;
	int i = 0;
	for(; i < count; i++)
	{
		// request
		mutex_lock(&bench_mutex);
		bench_counter++;
		cond_var_signal(&bench_cond);
		mutex_unlock(&bench_mutex);

		// wait
		mutex_lock(&bench_mutex2);
		if (bench_counter != bench_counter2)
			cond_var_wait(&bench_cond2, &bench_mutex2);
		mutex_unlock(&bench_mutex2);
	}

	// stop
	mutex_lock(&bench_mutex);
	bench_counter++;
	bench_stop = true;
	cond_var_signal(&bench_cond);
	mutex_unlock(&bench_mutex);

	// wait
	mutex_lock(&bench_mutex2);
	if (bench_counter != bench_counter2)
		cond_var_wait(&bench_cond2, &bench_mutex2);
	mutex_unlock(&bench_mutex2);

	time_end(&time_us);

	double sec = (double)time_us / 1000.0 / 1000.0;

	printf("%.2f sec (rps %.0f) ", sec, count / sec);

	mutex_free(&bench_mutex);
	mutex_free(&bench_mutex2);
	cond_var_free(&bench_cond);
	cond_var_free(&bench_cond2);

	task_wait(&task);
	task_free(&task);
}
