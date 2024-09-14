
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

static void
bench_connection(void* arg)
{
	BenchWorker* self = arg;
	Client* client = NULL;

	Exception e;
	if (enter(&e))
	{
		// create client and connect
		client = client_create();
		client_set_remote(client, self->bench->remote);
		client_connect(client);

		// process
		self->bench->iface->main(self, client);
	}

	if (leave(&e))
	{ }

	if (client)
	{
		client_close(client);
		client_free(client);
	}
}

static void
bench_worker_main(void* arg)
{
	BenchWorker* self = arg;

	// start connections
	int connections = self->connections;
	while (connections-- > 0)
		coroutine_create(bench_connection, self);

	// wait for stop
	for (;;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);
		if (msg->id == RPC_STOP)
			break;
	}

	// wait for completion
	self->shutdown = true;
	while (am_task->coroutine_mgr.count > 1)
		coroutine_yield();
}

static BenchWorker*
bench_worker_allocate(Bench* bench, int connections)
{
	auto self = (BenchWorker*)am_malloc(sizeof(BenchWorker));
	self->connections = connections;
	self->shutdown    = false;
	self->bench       = bench;
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

static void
bench_worker_free(BenchWorker* self)
{
	task_free(&self->task);
	am_free(self);
}

static void
bench_worker_start(BenchWorker* self)
{
	task_create(&self->task, "bench", bench_worker_main, self);
}

static void
bench_worker_stop_notify(BenchWorker* self)
{
	auto buf = msg_create_nothrow(self->task.buf_mgr, RPC_STOP, 0);
	if (! buf)
		abort();
	channel_write(&self->task.channel, buf);
}

static void
bench_worker_stop(BenchWorker* self)
{
	task_wait(&self->task);
}

void
bench_init(Bench* self, Remote* remote)
{
	memset(self, 0, sizeof(*self));
	self->remote = remote;
	vars_init(&self->vars);
	list_init(&self->list);
	VarDef defs[] =
	{
		{ "type",    VAR_STRING, VAR_C, &self->type,    "tpcb", 0   },
		{ "threads", VAR_INT,    VAR_C, &self->threads,  NULL,  4   },
		{ "clients", VAR_INT,    VAR_C, &self->clients,  NULL,  12  },
		{ "time",    VAR_INT,    VAR_C, &self->time,     NULL,  10  },
		{ "scale",   VAR_INT,    VAR_C, &self->scale,    NULL,  1   },
		{ "batch",   VAR_INT,    VAR_C, &self->batch,    NULL,  500 },
		{  NULL,     0,          0,     NULL,            NULL,  0   }
	};
	vars_define(&self->vars, defs);
}

void
bench_free(Bench* self)
{
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_free(worker);
	}
	vars_free(&self->vars);
}

static void
bench_service(Bench* self, bool create)
{
	Client* client = NULL;

	Exception e;
	if (enter(&e))
	{
		// create client and connect
		client = client_create();
		client_set_remote(client, self->remote);
		client_connect(client);

		// drop test schema if exists
		Str str;
		str_set_cstr(&str, "drop schema if exists __bench cascade");
		client_execute(client, &str);

		if (create)
		{
			// create test schema and run benchmark
			str_set_cstr(&str, "create schema __bench");
			client_execute(client, &str);

			self->iface->create(self, client);
		}
	}

	if (leave(&e))
	{ }

	if (client)
	{
		client_close(client);
		client_free(client);
	}
}

void
bench_run(Bench* self)
{
	// validate options
	auto type               = var_string_of(&self->type);
	auto scale              = var_int_of(&self->scale);
	auto time               = var_int_of(&self->time);
	auto workers            = var_int_of(&self->threads);
	auto clients            = var_int_of(&self->clients);
	auto clients_per_worker = clients / workers;

	// set benchmark
	if (str_compare_cstr(type, "tpcb"))
		self->iface = &bench_tpcb;
	else
	if (str_compare_cstr(type, "insert"))
		self->iface = &bench_insert;
	else
	if (str_compare_cstr(type, "upsert"))
		self->iface = &bench_upsert;
	else
		error("unknown benchmark type '%.*s'", str_size(type),
		      str_of(type));

	// hello
	info("amelie benchmark.");
	info("");
	info("type:    %.*s", str_size(type), str_of(type));
	info("time:    %" PRIu64 " sec", time);
	info("threads: %" PRIu64, workers);
	info("clients: %" PRIu64 " (%" PRIu64 " per thread)", clients, clients_per_worker);
	info("scale:   %" PRIu64, scale);
	info("");

	// prepare workers
	while (workers-- > 0)
	{
		auto worker = bench_worker_allocate(self, clients_per_worker);
		list_append(&self->list, &worker->link);
	}

	// prepare tables
	bench_service(self, true);

	// begin
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_start(worker);
	}

	uint64_t prev_tx = 0;
	uint64_t prev_wr = 0;
	for (auto duration = time; duration > 0; duration--)
	{
		coroutine_sleep(1000);
		auto tx = atomic_u64_of(&self->transactions);
		auto wr = atomic_u64_of(&self->writes);
		if (wr > 0)
			info("%" PRIu64 " transactions/sec, %.2f millions writes/sec (%" PRIu64 ")",
			     tx - prev_tx,
			     (float)(wr - prev_wr) / 1000000.0,
			     wr - prev_wr);
		else
			info("%" PRIu64 " transactions/sec", tx - prev_tx);
		prev_tx = tx;
		prev_wr = wr;
	}

	// stop
	list_foreach(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_stop_notify(worker);
	}
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_stop(worker);
	}

	// cleanup
	bench_service(self, false);

	// report
	info("");
	info("transactions: %" PRIu64, self->transactions);
	info("writes:       %.2f millions writes (%" PRIu64 ")",
	     self->writes / 1000000.0,
	     self->writes);
}
