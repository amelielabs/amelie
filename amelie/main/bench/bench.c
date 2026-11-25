
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_bench.h>

void
bench_init(Bench* self, Main* main)
{
	memset(self, 0, sizeof(*self));
	self->main = main;
	opts_init(&self->opts);
	OptsDef defs[6] =
	{
		{ "type",      OPT_STRING, OPT_C,       &self->opt_type,     "tpcb", 0     },
		{ "databases", OPT_INT,    OPT_C|OPT_Z, &self->opt_databases, NULL,  1     },
		{ "clients",   OPT_INT,    OPT_C|OPT_Z, &self->opt_clients,   NULL,  32    },
		{ "duration",  OPT_INT,    OPT_C|OPT_Z, &self->opt_duration,  NULL,  10    },
		{ "batch",     OPT_INT,    OPT_C|OPT_Z, &self->opt_batch,     NULL,  64    },
		{ "histogram", OPT_BOOL,   OPT_C,       &self->opt_histogram, NULL,  false },
	};
	opts_define(&self->opts, defs, 6);
}

void
bench_free(Bench* self)
{
	if (self->clients)
	{
		for (auto i = 0; i < self->clients_count; i++)
		{
			auto client = &self->clients[i];
			if (client->client)
				client_free(client->client);
			endpoint_free(&client->endpoint);
		}
		am_free(self->clients);
	}
	opts_free(&self->opts);
}

static void
bench_deploy_client(BenchClient* self, int db_id, bool deploy)
{
	auto endpoint_main = &self->bench->main->endpoint;
	auto endpoint = &self->endpoint;

	// set host/path
	auto path = &endpoint_main->path;
	auto host = &endpoint_main->host;
	if (! str_empty(path))
		str_assign(&endpoint->path, path);
	if (! str_empty(host))
		str_assign(&endpoint->host, host);
	endpoint->port = endpoint_main->port;

	// set db
	char name[16];
	format(name, sizeof(name), "db_{d}", db_id);
	str_assign_cstr(&endpoint->db, name);
	endpoint_auth(endpoint);

	// connnect
	self->client = client_allocate();
	client_set_endpoint(self->client, endpoint);
	if (self->bench->opt_histogram)
		client_set_histogram(self->client, &self->bench->histogram);
	client_connect(self->client);

	// create relations
	if (deploy)
		self->bench->iface->deploy(self);
}

static void
bench_create(Bench* self)
{
	// create databases
	auto databases = self->opt_databases;
	auto client = client_allocate();
	defer(client_free, client);
	client_set_endpoint(client, &self->main->endpoint);
	client_connect(client);
	for (auto i = 0; i < databases; i++)
	{
		char name[16];
		format(name, sizeof(name), "db_{d}", i);
		Str str;
		str_set_cstr(&str, name);

		char cmd[256];
		format(cmd, sizeof(cmd), "drop database if exists {str}", &str);
		client_run(client, cmd);

		format(cmd, sizeof(cmd), "create database {str}", &str);
		client_run(client, cmd);
	}
}

static void
bench_deploy(Bench* self)
{
	// prepare clients
	auto databases = (int)self->opt_databases;
	auto clients   = (int)self->opt_clients;
	auto size      = sizeof(BenchClient) * databases * clients;

	// create databases
	bench_create(self);

	self->clients_count = databases * clients;
	self->clients = am_malloc(size);
	for (auto i = 0; i < self->clients_count; i++)
	{
		auto at = &self->clients[i];
		at->client = NULL;
		at->bench  = self;
		endpoint_init(&at->endpoint);
	}

	// connect clients
	auto seq = 0;
	for (auto i = 0; i < databases; i++)
	{
		auto at = &self->clients[seq];
		bench_deploy_client(at, i, true);
		seq++;
		for (auto j = 1; j < clients; j++)
		{
			at = &self->clients[seq];
			bench_deploy_client(at, i, false);
			seq++;
		}
	}
}

static void
bench_client(void* arg)
{
	BenchClient* self = arg;
	error_catch (
		self->bench->iface->main(self);
	);
}

static void
bench_main(Bench* self)
{
	// start
	for (auto i = 0; i < self->clients_count; i++)
	{
		auto client = &self->clients[i];
		coroutine_create(bench_client, client);
	}

	// report
	auto duration = self->opt_duration;
	uint64_t prev_tx = 0;
	uint64_t prev_wr = 0;
	for (; duration > 0; duration--)
	{
		coroutine_sleep(1000);
		auto tx = self->transactions;
		auto wr = self->writes;
		if (wr > 0)
			info("{u64} transactions/sec, {.2f} millions writes/sec ({u64})",
			     tx - prev_tx,
			     (float)(wr - prev_wr) / 1000000.0,
			     wr - prev_wr);
		else
			info("{u64} transactions/sec", tx - prev_tx);
		prev_tx = tx;
		prev_wr = wr;
	}

	// stop and wait for completion
	self->shutdown = true;
	while (am_task->coroutines.count > 1)
		coroutine_yield();

	// report
	info("");
	info("transactions: {u64}", self->transactions);
	info("writes:       {.2f} millions writes ({u64})",
	     self->writes / 1000000.0,
	     self->writes);
	info("");
	auto histogram = self->opt_histogram;
	if (histogram)
		histogram_print(&self->histogram);
}

void
bench_run(Bench* self)
{
	info("");
	info("amelie benchmark.");
	info("");
	info("type:      {str}",     &self->opt_type);
	info("duration:  {i64} sec", self->opt_duration);
	info("databases: {i64} ",    self->opt_databases);
	info("clients:   {i64} ",    self->opt_clients);
	info("batch:     {i64}",     self->opt_batch);
	info("");

	// set benchmark
	auto type = &self->opt_type;
	if (str_is_cstr(type, "insert"))
		self->iface = &bench_insert;
	else
	if (str_is_cstr(type, "upsert"))
		self->iface = &bench_upsert;
	else
	if (str_is_cstr(type, "tpcb"))
		self->iface = &bench_tpcb;
	else
	if (str_is_cstr(type, "decre"))
		self->iface = &bench_decre;
	else
	if (str_is_cstr(type, "vector"))
		self->iface = &bench_vector;
	else
		error("unknown benchmark type '{str}'", type);

	// create clients and deploy
	bench_deploy(self);
	bench_main(self);
}
