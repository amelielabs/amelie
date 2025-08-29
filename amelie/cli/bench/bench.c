
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_bench.h>

static void
bench_connection(void* arg)
{
	BenchWorker* self = arg;
	auto client = bench_client_create(self->bench->iface_client, self->bench->amelie);
	defer(bench_client_free, client);

	// meassure histogram
	if (opt_int_of(&self->bench->histogram))
		bench_client_set(client, &self->histogram);

	error_catch
	(
		// create client and connect
		bench_client_connect(client, self->bench->remote);

		// process
		self->bench->iface->main(self, client);
	);
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
	channel_read(&am_task->channel, -1);

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
	histogram_init(&self->histogram);
	msg_init(&self->stop, MSG_STOP);
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
	channel_write(&self->task.channel, &self->stop);
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
	self->iface        = NULL;
	self->iface_client = &bench_client_http;
	self->amelie       = NULL;
	self->remote       = remote;
	opts_init(&self->opts);
	list_init(&self->list);
	OptsDef defs[] =
	{
		{ "type",      OPT_STRING, OPT_C,       &self->type,     "tpcb", 0     },
		{ "threads",   OPT_INT,    OPT_C|OPT_Z, &self->threads,   NULL,  1     },
		{ "clients",   OPT_INT,    OPT_C|OPT_Z, &self->clients,   NULL,  12    },
		{ "time",      OPT_INT,    OPT_C|OPT_Z, &self->time,      NULL,  10    },
		{ "scale",     OPT_INT,    OPT_C|OPT_Z, &self->scale,     NULL,  1     },
		{ "batch",     OPT_INT,    OPT_C|OPT_Z, &self->batch,     NULL,  500   },
		{ "init",      OPT_BOOL,   OPT_C,       &self->init,      NULL,  true  },
		{ "unlogged",  OPT_BOOL,   OPT_C,       &self->unlogged,  NULL,  false },
		{ "histogram", OPT_BOOL,   OPT_C,       &self->histogram, NULL,  false },
		{ "api",       OPT_BOOL,   OPT_C,       &self->api,       NULL,  false },
		{  NULL,       0,          0,            NULL,            NULL,  0     }
	};
	opts_define(&self->opts, defs);
}

void
bench_free(Bench* self)
{
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_free(worker);
	}
	opts_free(&self->opts);
	if (self->amelie)
		amelie_free(self->amelie);
}

static void
bench_service_execute(Bench* self, BenchClient* client, bool create)
{
	// drop test schema if exists
	Str str;
	str_set_cstr(&str, "drop schema if exists __bench cascade");
	bench_client_execute(client, &str);

	if (create)
	{
		// create test schema and run benchmark
		str_set_cstr(&str, "create schema __bench");
		bench_client_execute(client, &str);

		self->iface->create(self, client);
	}
}

static void
bench_service(Bench* self, bool create)
{
	auto client = bench_client_create(self->iface_client, self->amelie);
	defer(bench_client_free, client);
	error_catch
	(
		// create client and connect
		bench_client_connect(client, self->remote);

		bench_service_execute(self, client, create);
	);
}

void
bench_run(Bench* self)
{
	// validate options
	auto type               = opt_string_of(&self->type);
	auto scale              = opt_int_of(&self->scale);
	auto batch              = opt_int_of(&self->batch);
	auto time               = opt_int_of(&self->time);
	auto workers            = opt_int_of(&self->threads);
	auto clients            = opt_int_of(&self->clients);
	auto clients_per_worker = clients / workers;
	auto init               = opt_int_of(&self->init);
	auto unlogged           = opt_int_of(&self->unlogged);
	auto histogram          = opt_int_of(&self->histogram);
	auto api                = opt_int_of(&self->api);

	// set benchmark
	if (str_is_cstr(type, "tpcb"))
		self->iface = &bench_tpcb;
	else
	if (str_is_cstr(type, "import"))
		self->iface = &bench_import;
	else
	if (str_is_cstr(type, "insert"))
		self->iface = &bench_insert;
	else
	if (str_is_cstr(type, "upsert"))
		self->iface = &bench_upsert;
	else
	if (str_is_cstr(type, "resolved"))
		self->iface = &bench_resolved;
	else
	if (str_is_cstr(type, "decre"))
		self->iface = &bench_decre;
	else
		error("unknown benchmark type '%.*s'", str_size(type),
		      str_of(type));

	// hello
	info("amelie benchmark.");
	info("");

	// using embeddable database benchmark
	if (api)
	{
		auto path = remote_get(self->remote, REMOTE_PATH);
		if (str_empty(path))
			error("a repository path must be specified for the embeddable db benchmark");

		// get the directory name from the socket path <path>/socket
		if (str_size(path) < 7 || memcmp(path->end - 7, "/socket", 7) != 0)
			error("invalid repository path");
		str_truncate(path, 7);
		*path->end = 0;

		// open the repository
		self->amelie = amelie_init();
		if (! self->amelie)
			error("amelie_init() failed");
		int   argc   = 1;
		char* argv[] = {
			"--log_to_stdout=false",
		};
		auto rc = amelie_open(self->amelie, str_of(path), argc, argv);
		if (rc == -1)
			error("amelie_open() failed");

		// update client interface
		self->iface_client = &bench_client_api;
	}

	info("type:      %.*s", str_size(type), str_of(type));
	info("time:      %" PRIu64 " sec", time);
	info("threads:   %" PRIu64, workers);
	info("clients:   %" PRIu64 " (%" PRIu64 " per thread)", clients, clients_per_worker);
	info("batch:     %" PRIu64, batch);
	info("scale:     %" PRIu64, scale);
	info("init:      %" PRIu64, init);
	info("unlogged:  %" PRIu64, unlogged);
	info("histogram: %" PRIu64, histogram);
	info("api:       %" PRIu64, api);
	info("");

	// prepare workers
	while (workers-- > 0)
	{
		auto worker = bench_worker_allocate(self, clients_per_worker);
		list_append(&self->list, &worker->link);
	}

	// prepare tables
	if (init)
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
	list_foreach(&self->list)
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

	if (histogram)
	{
		list_foreach_safe(&self->list)
		{
			auto worker = list_at(BenchWorker, link);
			histogram_print(&worker->histogram);
		}
	}
}
