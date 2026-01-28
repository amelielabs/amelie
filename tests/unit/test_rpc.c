
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

#include <amelie_test>

static void
test_rpc_main(void* arg)
{
	unused(arg);
	auto msg = task_recv();
	auto rpc = rpc_of(msg);
	test(rpc->argc == 2);

	*(int*)rpc_arg_ptr(rpc, 0) = 123;
	test(! memcmp(rpc_arg_ptr(rpc, 1), "hello", 5));
	rpc_signal(rpc);
}

void
test_rpc(void* arg)
{
	unused(arg);
	Task task;
	task_init(&task);
	task_create(&task, "test", test_rpc_main, NULL);

	int rc = 0;
	rpc(&task, 0, 2, &rc, "hello");
	test(rc == 123);

	task_wait(&task);
	task_free(&task);
}

static void
test_rpc_execute_cb(Rpc* self, void* arg)
{
	unused(arg);
	test(self->argc == 2);

	*((int*)rpc_arg_ptr(self, 0)) = 123;
	test(! memcmp(rpc_arg_ptr(self, 1), "hello", 5));
}

static void
test_rpc_execute_main(void* arg)
{
	unused(arg);
	auto msg = task_recv();
	rpc_execute(rpc_of(msg), test_rpc_execute_cb, NULL);
}

void
test_rpc_execute(void* arg)
{
	unused(arg);
	Task task;
	task_init(&task);
	task_create(&task, "test", test_rpc_execute_main, NULL);

	int rc = 0;
	rpc(&task, 0, 2, &rc, "hello");
	test(rc == 123);

	task_wait(&task);
	task_free(&task);
}

static void
test_rpc_execute_error_cb(Rpc* rpc, void* arg)
{
	unused(arg);
	unused(rpc);
	error("test");
}

static void
test_rpc_execute_error_main(void* arg)
{
	unused(arg);
	auto msg = task_recv();
	rpc_execute(rpc_of(msg), test_rpc_execute_error_cb, NULL);
}

void
test_rpc_execute_error(void* arg)
{
	unused(arg);
	Task task;
	task_init(&task);
	task_create(&task, "test", test_rpc_execute_error_main, NULL);

	auto on_error = error_catch( rpc(&task, 0, 0) );
	test(on_error);

	task_wait(&task);
	task_free(&task);
}
