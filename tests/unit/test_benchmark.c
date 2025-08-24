
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

static void
test_benchmark_main(void* arg)
{
	unused(arg);
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto rpc = rpc_of(buf);
		buf_free(buf);
		stop = rpc->id == 0;
		rpc_done(rpc);
	}
}

void
test_benchmark(void* arg)
{
	unused(arg);
	Task task;
	task_init(&task);
	task_create(&task, "test", test_benchmark_main, NULL);

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
test_benchmark_mutex_main(void* arg)
{
	unused(arg);
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
test_benchmark_mutex(void* arg)
{
	unused(arg);
	Task task;
	task_init(&task);
	task_create(&task, "test", test_benchmark_mutex_main, NULL);

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

static Buf*
test_benchmark_get_buf(void)
{
	// obj
	auto buf = buf_create();
	encode_obj(buf);

	// active
	encode_raw(buf, "active", 6);
	encode_bool(buf, 0);

	// rotate_wm
	encode_raw(buf, "rotate_wm", 9);
	encode_integer(buf, 0);

	// sync_on_rotate
	encode_raw(buf, "sync_on_rotate", 14);
	encode_bool(buf, false);

	// sync_on_write
	encode_raw(buf, "sync_on_write", 13);
	encode_bool(buf, false);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, 0);

	// lsn_min
	encode_raw(buf, "lsn_min", 7);
	encode_integer(buf, 0);

	// files
	encode_raw(buf, "files", 5);
	encode_integer(buf, 0);

	encode_obj_end(buf);
	return buf;
}

static Buf*
test_benchmark_get_buf_defer(void)
{
	// obj
	auto buf = buf_create();
	errdefer_buf(buf);

	encode_obj(buf);

	// active
	encode_raw(buf, "active", 6);
	encode_bool(buf, 0);

	// rotate_wm
	encode_raw(buf, "rotate_wm", 9);
	encode_integer(buf, 0);

	// sync_on_rotate
	encode_raw(buf, "sync_on_rotate", 14);
	encode_bool(buf, false);

	// sync_on_write
	encode_raw(buf, "sync_on_write", 13);
	encode_bool(buf, false);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, 0);

	// lsn_min
	encode_raw(buf, "lsn_min", 7);
	encode_integer(buf, 0);

	// files
	encode_raw(buf, "files", 5);
	encode_integer(buf, 0);

	encode_obj_end(buf);
	return buf;
}

void
test_benchmark_buf(void* arg)
{
	unused(arg);
	uint64_t time_us;
	time_start(&time_us);

	int count = 30000000;
	int i = 0;
	for(; i < count; i++)
	{
		auto buf = test_benchmark_get_buf();
		buf_free(buf);
	}

	time_end(&time_us);
	double sec = (double)time_us / 1000.0 / 1000.0;

	printf("%.2f sec (rps %.0f) ", sec, count / sec);
}

void
test_benchmark_buf_defer(void* arg)
{
	unused(arg);
	uint64_t time_us;
	time_start(&time_us);

	int count = 30000000;
	int i = 0;
	for(; i < count; i++)
	{
		auto buf = test_benchmark_get_buf_defer();
		buf_free(buf);
	}

	time_end(&time_us);
	double sec = (double)time_us / 1000.0 / 1000.0;

	printf("%.2f sec (rps %.0f) ", sec, count / sec);
}
